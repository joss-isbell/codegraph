param(
  [Parameter(Mandatory=$true)][string]$Plan,
  [Parameter(Mandatory=$true)][string]$LaunchOutput,
  [ValidateRange(1,3600)][int]$DeadlineSeconds = 900,
  [string]$Python = 'C:/Users/atramenta/AppData/Local/Programs/Python/Python312/python.exe'
)
$ErrorActionPreference = 'Stop'
$saved = Get-Content -LiteralPath $Plan -Raw | ConvertFrom-Json
$prefix = [string]$saved.sourceBoundCache.prefix
if (-not $prefix -or (Test-Path -LiteralPath $prefix)) { throw 'Declare a fresh absent sourceBoundCache.prefix.' }
New-Item -ItemType Directory -Path $LaunchOutput -ErrorAction Stop | Out-Null
$resultPath = Join-Path $LaunchOutput 'launch-result.json'
# Preserve a failed/incomplete attempt even if compilation or launcher setup fails.
@{ exitCode=$null; cleanupComplete=$false; errors=@('Launcher did not complete.') } |
  ConvertTo-Json | Set-Content -LiteralPath $resultPath -Encoding utf8
Add-Type -TypeDefinition @'
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
public sealed class NormalLaunch {
  [StructLayout(LayoutKind.Sequential)] struct Limits {
    public long PerProcessUserTimeLimit, PerJobUserTimeLimit;
    public uint LimitFlags;
    public UIntPtr MinimumWorkingSetSize, MaximumWorkingSetSize;
    public uint ActiveProcessLimit;
    public UIntPtr Affinity;
    public uint PriorityClass, SchedulingClass;
  }
  [StructLayout(LayoutKind.Sequential)] struct Extended {
    public Limits BasicLimitInformation;
    public ulong ReadOperationCount, WriteOperationCount, OtherOperationCount;
    public ulong ReadTransferCount, WriteTransferCount, OtherTransferCount;
    public UIntPtr ProcessMemoryLimit, JobMemoryLimit, PeakProcessMemoryUsed, PeakJobMemoryUsed;
  }
  [DllImport("kernel32.dll", SetLastError=true, CharSet=CharSet.Unicode)]
  static extern IntPtr CreateJobObjectW(IntPtr attributes, string name);
  [DllImport("kernel32.dll", SetLastError=true)]
  static extern bool SetInformationJobObject(IntPtr job, int infoClass, ref Extended info, uint size);
  [DllImport("kernel32.dll", SetLastError=true)]
  static extern bool AssignProcessToJobObject(IntPtr job, IntPtr process);
  [DllImport("kernel32.dll", SetLastError=true)]
  static extern bool QueryInformationJobObject(IntPtr job, int infoClass, IntPtr info, uint size, IntPtr returned);
  [DllImport("kernel32.dll", SetLastError=true)]
  static extern bool TerminateJobObject(IntPtr job, uint exitCode);
  [DllImport("kernel32.dll", SetLastError=true)]
  static extern bool CloseHandle(IntPtr handle);
  public sealed class Result {
    public int? pid, exitCode;
    public bool deadline, overflow, logsClosed, jobAssigned, cleanupComplete, jobHandleClosed, processHandleClosed, pipeHandlesClosed;
    public long stdoutBytes, stderrBytes;
    public double elapsedSeconds;
    public long[] remainingPids = new long[0];
    public List<string> errors = new List<string>();
  }
  static void Error(Result result, string context, Exception error) {
    string message = context + ": " + error.GetType().Name + ": " + error.Message;
    result.errors.Add(message.Substring(0, Math.Min(2048, message.Length)));
  }
  static long[] Members(IntPtr job) {
    const int size = 65536;
    IntPtr data = Marshal.AllocHGlobal(size);
    try {
      if (!QueryInformationJobObject(job, 3, data, size, IntPtr.Zero)) throw new Win32Exception();
      uint assigned = unchecked((uint)Marshal.ReadInt32(data, 0));
      uint count = unchecked((uint)Marshal.ReadInt32(data, 4));
      if (assigned != count || count > (size - 8) / IntPtr.Size)
        throw new InvalidOperationException("Owned Job inventory incomplete or overflowing.");
      var pids = new long[count];
      for (int index = 0; index < pids.Length; index++)
        pids[index] = Marshal.ReadIntPtr(data, 8 + index * IntPtr.Size).ToInt64();
      return pids;
    } finally { Marshal.FreeHGlobal(data); }
  }
  public long StdoutBytes, StderrBytes;
  public int Overflow;
  async Task Capture(Stream source, string file, bool stderr) {
    using (var output = new FileStream(file, FileMode.CreateNew, FileAccess.Write, FileShare.Read)) {
      var buffer = new byte[4096]; long total = 0; int count;
      while ((count = await source.ReadAsync(buffer, 0, buffer.Length)) != 0) {
        int retain = (int)Math.Min(count, Math.Max(0, 1048576 - total));
        if (retain > 0) { await output.WriteAsync(buffer, 0, retain); await output.FlushAsync(); }
        total += count;
        if (stderr) Interlocked.Exchange(ref StderrBytes, total); else Interlocked.Exchange(ref StdoutBytes, total);
        if (total > 1048576) Interlocked.Exchange(ref Overflow, 1);
      }
    }
  }
  public Result Run(ProcessStartInfo start, string argv, string output, int seconds) {
    var result = new Result();
    var elapsed = Stopwatch.StartNew();
    var process = new Process();
    IntPtr job = IntPtr.Zero;
    Task stdout = null, stderr = null;
    bool started = false;
    try {
      // Null security attributes make this handle noninheritable. The pwsh
      // process alone owns it: caller death closes it and kills all descendants.
      job = CreateJobObjectW(IntPtr.Zero, null);
      if (job == IntPtr.Zero) throw new Win32Exception();
      var limits = new Extended();
      limits.BasicLimitInformation.LimitFlags = 0x2000; // KILL_ON_JOB_CLOSE; no breakaway.
      if (!SetInformationJobObject(job, 9, ref limits, (uint)Marshal.SizeOf<Extended>())) throw new Win32Exception();
      process.StartInfo = start;
      if (!process.Start()) throw new InvalidOperationException("Normal wrapper did not start.");
      started = true;
      result.pid = process.Id;
      stdout = Capture(process.StandardOutput.BaseStream, Path.Combine(output, "stdout.log"), false);
      stderr = Capture(process.StandardError.BaseStream, Path.Combine(output, "stderr.log"), true);
      // The gate imports stdlib only, then blocks on stdin. No target (including
      // normal.py's bootstrap Git) runs before exact-handle Job assignment.
      if (!AssignProcessToJobObject(job, process.Handle)) throw new Win32Exception();
      result.jobAssigned = true;
      process.StandardInput.WriteLine(argv);
      process.StandardInput.Close();
      while (true) {
        bool exited = process.WaitForExit(100);
        // Check after the wait even when the process exited during that wait.
        result.deadline = elapsed.Elapsed.TotalSeconds >= seconds;
        if (exited || result.deadline || Volatile.Read(ref Overflow) != 0 || stdout.IsFaulted || stderr.IsFaulted) break;
      }
    } catch (Exception error) { Error(result, "launch", error); }
    finally {
      result.deadline |= elapsed.Elapsed.TotalSeconds >= seconds;
      // Cleanup is exact-handle/Job-only, never a PID scan or global tree kill.
      try { if (started) process.StandardInput.Close(); }
      catch (Exception error) { Error(result, "gate stdin close", error); }
      // Assignment failure leaves only the unreleased gate outside the Job.
      // Keep this independent of inventory operations, which can themselves fail.
      if (started && !result.jobAssigned) {
        try {
          if (!process.HasExited) process.Kill();
          if (!process.WaitForExit(10000)) throw new TimeoutException("Unreleased gate did not exit.");
          result.exitCode = process.ExitCode;
        } catch (Exception error) { Error(result, "unreleased gate cleanup", error); }
      }
      try {
        if (job != IntPtr.Zero) {
          result.remainingPids = Members(job);
          if (result.remainingPids.Length != 0) {
            if (started && process.HasExited && process.ExitCode == 0 && !result.deadline && Overflow == 0)
              result.errors.Add("Normal runner exited with owned descendants still in the outer Job.");
            if (!TerminateJobObject(job, 124)) throw new Win32Exception();
          }
        }
        var cleanup = Stopwatch.StartNew();
        while (job != IntPtr.Zero && (result.remainingPids = Members(job)).Length != 0 && cleanup.ElapsedMilliseconds < 10000)
          Thread.Sleep(50);
        bool exited = !started || process.WaitForExit((int)Math.Max(0, 10000 - cleanup.ElapsedMilliseconds));
        result.cleanupComplete = exited && job != IntPtr.Zero && result.remainingPids.Length == 0;
        if (started && exited) result.exitCode = process.ExitCode;
        if (!result.cleanupComplete) result.errors.Add("Owned process cleanup incomplete.");
      } catch (Exception error) { Error(result, "owned cleanup", error); }
      finally {
        if (job != IntPtr.Zero) {
          result.jobHandleClosed = CloseHandle(job);
          if (!result.jobHandleClosed) Error(result, "Job handle close", new Win32Exception());
        }
      }
      try {
        var captures = new List<Task>();
        if (stdout != null) captures.Add(stdout);
        if (stderr != null) captures.Add(stderr);
        result.logsClosed = Task.WaitAll(captures.ToArray(), 10000);
        if (!result.logsClosed) result.errors.Add("Log capture did not drain before cleanup deadline.");
      } catch (Exception error) { Error(result, "log capture", error); }
      try {
        if (started) { process.StandardOutput.Close(); process.StandardError.Close(); }
        result.pipeHandlesClosed = true;
      } catch (Exception error) { Error(result, "pipe handle close", error); }
      try { process.Dispose(); result.processHandleClosed = true; }
      catch (Exception error) { Error(result, "process handle close", error); }
      result.overflow = Volatile.Read(ref Overflow) != 0;
      result.stdoutBytes = Interlocked.Read(ref StdoutBytes);
      result.stderrBytes = Interlocked.Read(ref StderrBytes);
      result.elapsedSeconds = elapsed.Elapsed.TotalSeconds;
    }
    return result;
  }
}
'@
$start = [System.Diagnostics.ProcessStartInfo]::new()
$start.FileName = $Python
$start.WorkingDirectory = [string]$saved.cwd
$start.UseShellExecute = $false
$start.CreateNoWindow = $true
$start.RedirectStandardInput = $true
$start.RedirectStandardOutput = $true
$start.RedirectStandardError = $true
# Same JSON-line gate protocol as runner.py _gate, without importing the runner
# before normal.py's raw-source bootstrap. Only the owned gate starts pre-assign.
$gate = 'import json, subprocess, sys; sys.exit(subprocess.call(json.loads(sys.stdin.readline())))'
foreach ($arg in @('-I','-S','-B','-X',"pycache_prefix=$prefix",'-c',$gate)) { $start.ArgumentList.Add($arg) }
$targetArgv = @($Python,'-I','-S','-B','-X',"pycache_prefix=$prefix",(Join-Path $PSScriptRoot 'normal.py'),(Resolve-Path -LiteralPath $Plan).Path) | ConvertTo-Json -Compress
$start.Environment['PYTHONPYCACHEPREFIX'] = $prefix
$start.Environment['PYTHONDONTWRITEBYTECODE'] = '1'
$start.Environment['PYTHONUTF8'] = '1'
$start.Environment['PYTHONIOENCODING'] = 'utf-8'
$start.Environment['GIT_CONFIG_COUNT'] = '1'
$start.Environment['GIT_CONFIG_KEY_0'] = 'safe.directory'
$start.Environment['GIT_CONFIG_VALUE_0'] = [string]$saved.sourceRoot
$result = [NormalLaunch]::new().Run($start, $targetArgv, $LaunchOutput, $DeadlineSeconds)
$result | ConvertTo-Json | Set-Content -LiteralPath $resultPath -Encoding utf8
$result | ConvertTo-Json -Compress | Write-Output
if ($result.exitCode -ne 0 -or $result.deadline -or $result.overflow -or -not $result.logsClosed -or
    -not $result.jobAssigned -or -not $result.cleanupComplete -or -not $result.jobHandleClosed -or
    -not $result.processHandleClosed -or -not $result.pipeHandlesClosed -or $result.errors.Count -ne 0) {
  foreach ($stream in @('stdout.log','stderr.log')) {
    $logPath = Join-Path $LaunchOutput $stream
    if (Test-Path -LiteralPath $logPath) {
      $log = Get-Content -LiteralPath $logPath -Raw
      if ($log) { Write-Output $log.Substring([Math]::Max(0,$log.Length-8192)) }
    }
  }
  exit 1
}
exit 0
