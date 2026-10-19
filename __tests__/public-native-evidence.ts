import * as fs from 'fs';
import * as path from 'path';
import { getWriterPidPath } from '../src/mcp/writer-lock';

const OBSERVATION_BYTES = 4194304;
const OWNER_BYTES = 4096;
const SNAPSHOTS = 64;
const detail = (error: unknown) => String(error).slice(0, 4096);

interface FileCapture {
  text: string | null;
  seenBytes: number;
  retainedBytes: number;
  truncated: boolean;
  failure: string | null;
}
interface OwnerObservation {
  root: string;
  writer: string | null;
  daemon: string | null;
  writerCapture: FileCapture;
  daemonCapture: FileCapture;
}

/** Bounded, sticky evidence for the two public native capacity/waiting suites. */
export class PublicNativeEvidence {
  private retainedEvents: any[] = [];
  private rawObservations = Buffer.alloc(0);
  private observationSeenBytes = 0;
  private observationLastReadBytes = 0;
  private observationRemovalAllowed = false;
  private observationFailure: string | null = null;
  private ownershipFailure: string | null = null;
  private persistenceFailure: string | null = null;
  private timelineFailure: string | null = null;
  private readonly ownershipSnapshots: Array<{ stage: string; monotonicNs: string; owners: OwnerObservation[] }> = [];
  private omittedSnapshots = 0;

  constructor(private readonly roots: string[], private readonly observations: string, private readonly outputName: string) {}

  get failed(): boolean {
    return !!(this.observationFailure || this.ownershipFailure || this.persistenceFailure || this.timelineFailure);
  }

  private read(file: string, maximum: number): FileCapture & { bytes: Buffer } {
    let descriptor: number | undefined;
    let prefix = Buffer.alloc(0);
    let seenBytes = 0;
    try {
      descriptor = fs.openSync(file, 'r');
      const stat = fs.fstatSync(descriptor);
      seenBytes = stat.size;
      if (!stat.isFile()) throw new Error('evidence path is not a regular file');
      const buffer = Buffer.alloc(Math.min(seenBytes, maximum));
      let retained = 0;
      while (retained < buffer.length) {
        const count = fs.readSync(descriptor, buffer, retained, buffer.length - retained, retained);
        if (!count) throw new Error('evidence file ended before its observed byte length');
        retained += count;
        prefix = buffer.subarray(0, retained);
      }
      return { bytes: prefix, text: prefix.toString(), seenBytes, retainedBytes: prefix.length, truncated: seenBytes > maximum,
        failure: seenBytes > maximum ? `evidence file exceeded ${maximum} bytes` : null };
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'ENOENT' && descriptor === undefined) {
        return { bytes: prefix, text: null, seenBytes: 0, retainedBytes: 0, truncated: false, failure: null };
      }
      return { bytes: prefix, text: prefix.toString(), seenBytes, retainedBytes: prefix.length, truncated: seenBytes > prefix.length, failure: detail(error) };
    } finally {
      if (descriptor !== undefined) {
        try { fs.closeSync(descriptor); }
        catch (error) { this.persistenceFailure ??= `evidence descriptor close: ${detail(error)}`; }
      }
    }
  }

  events(): any[] {
    if (this.observationFailure) throw new Error(this.observationFailure);
    const capture = this.read(this.observations, OBSERVATION_BYTES);
    if (capture.text === null) {
      if (this.observationSeenBytes && !this.observationRemovalAllowed) {
        this.observationFailure = 'worker observation disappeared before verified fixture removal';
        throw new Error(this.observationFailure);
      }
      return this.retainedEvents;
    }
    this.observationLastReadBytes = capture.seenBytes;
    try {
      if (capture.failure) {
        if (capture.bytes.length) this.rawObservations = capture.bytes;
        this.observationSeenBytes = Math.max(this.observationSeenBytes, capture.seenBytes);
        throw new Error(capture.failure);
      }
      if (capture.seenBytes < this.observationSeenBytes) throw new Error('worker observation byte length decreased');
      this.rawObservations = capture.bytes;
      this.observationSeenBytes = Math.max(this.observationSeenBytes, capture.seenBytes);
      this.retainedEvents = capture.text.split(/\r?\n/).filter(Boolean).map(line => JSON.parse(line));
      return this.retainedEvents;
    } catch (error) {
      this.observationFailure ??= `worker observation: ${detail(error)}`;
      throw new Error(this.observationFailure);
    }
  }

  allowObservationRemoval(): void { this.observationRemovalAllowed = true; }

  owners(): OwnerObservation[] {
    return this.roots.map(root => {
      const { bytes: _writerBytes, ...writerCapture } = this.read(getWriterPidPath(root), OWNER_BYTES);
      const { bytes: _daemonBytes, ...daemonCapture } = this.read(path.join(root, '.codegraph/daemon.pid'), OWNER_BYTES);
      for (const [name, capture] of [['writer', writerCapture], ['daemon', daemonCapture]] as const) {
        if (capture.failure) this.ownershipFailure ??= `${name} owner ${root}: ${capture.failure}`;
      }
      return { root, writer: writerCapture.text, daemon: daemonCapture.text, writerCapture, daemonCapture };
    });
  }

  failures() {
    return { observation: this.observationFailure, ownership: this.ownershipFailure,
      persistence: this.persistenceFailure, timeline: this.timelineFailure };
  }

  /** Never interrupts cleanup or replaces the caller's primary failure. */
  persist(stage: string, facts: () => object): void {
    try {
      const owners = this.owners();
      if (this.ownershipSnapshots.length < SNAPSHOTS) {
        this.ownershipSnapshots.push({ stage, monotonicNs: process.hrtime.bigint().toString(), owners });
      } else {
        this.omittedSnapshots++;
        this.timelineFailure ??= `ownership evidence exceeded ${SNAPSHOTS} snapshots`;
      }
      try { this.events(); } catch { /* sticky raw failure retained below */ }
      let value: object = {};
      try { value = facts(); } catch (error) { this.persistenceFailure ??= `facts capture: ${detail(error)}`; }
      if (!process.env.VALIDATION_OUT) return;
      fs.writeFileSync(path.join(process.env.VALIDATION_OUT, this.outputName), JSON.stringify({ ...value, stage,
        ownership: owners, ownershipSnapshots: this.ownershipSnapshots, omittedOwnershipSnapshots: this.omittedSnapshots,
        events: this.retainedEvents, observationCapture: { path: this.observations, encoding: 'base64', prefix: this.rawObservations.toString('base64'),
          seenBytes: this.observationSeenBytes, lastReadBytes: this.observationLastReadBytes, retainedBytes: this.rawObservations.length,
          truncated: this.observationSeenBytes > this.rawObservations.length, failure: this.observationFailure },
        evidenceFailures: this.failures(),
      }, null, 2));
    } catch (error) {
      this.persistenceFailure ??= `evidence persistence: ${detail(error)}`;
      try { process.stderr.write(`Public native evidence failure (${stage.slice(0, 128)}): ${this.persistenceFailure}\n`); } catch { /* keep cleanup running */ }
    }
  }
}
