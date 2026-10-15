# Issue tracker: GitHub

Issues and specs for this repo live in [joss-isbell/codegraph](https://github.com/joss-isbell/codegraph/issues). Use the `gh` CLI. Always target this repository explicitly with `--repo joss-isbell/codegraph` or the literal REST paths below; do not infer the repository from upstream metadata or the current directory.

## Conventions

- **Create an issue**: `gh issue create --repo joss-isbell/codegraph --title "<title>" --body-file issue-body.md`.
- **Read an issue**: `gh issue view <number> --repo joss-isbell/codegraph --json number,title,body,state,labels,assignees,comments,url`.
- **List issues**: `gh issue list --repo joss-isbell/codegraph --state open --json number,title,labels,assignees,url`, with appropriate `--label`, `--state`, and `--limit` filters. Use the paginated child query below for a complete Wayfinder frontier.
- **Comment**: `gh issue comment <number> --repo joss-isbell/codegraph --body-file answer.md`.
- **Apply / remove labels**: `gh issue edit <number> --repo joss-isbell/codegraph --add-label "<label>"` / `gh issue edit <number> --repo joss-isbell/codegraph --remove-label "<label>"`.
- **Edit the body**: `gh issue edit <number> --repo joss-isbell/codegraph --body-file issue-body.md`.
- **Close**: `gh issue close <number> --repo joss-isbell/codegraph`.

For multiline bodies or comments, save UTF-8 text to a file and use `--body-file`; do not use shell heredocs in PowerShell.

## Pull requests as a triage surface

**PRs as a request surface: no.** Set to `yes` only if this repo should treat external PRs as feature requests.

## When a skill says "publish to the issue tracker"

Create a GitHub issue in `joss-isbell/codegraph`.

## When a skill says "fetch the relevant ticket"

Read it as in **Read an issue** above.

## Wayfinding operations

Used by `/wayfinder`. The map is one issue; its tickets are native GitHub sub-issues. Blocking uses native GitHub issue dependencies. Do not substitute task lists or body-only dependency lines.

### Map and child tickets

- **Map**: create an issue with `--label wayfinder:map`, using the map body from the Wayfinder skill.
- **Child ticket**: create an issue with one `wayfinder:<type>` label: `wayfinder:research`, `wayfinder:prototype`, `wayfinder:grilling`, or `wayfinder:task`. Maps and tickets carry only their Wayfinder labels.
- **Parent relationship**: after creating a child, obtain its numeric database ID, then add it to the map:

  ```powershell
  gh api repos/joss-isbell/codegraph/issues/<child-number> --jq .id
  gh api --method POST repos/joss-isbell/codegraph/issues/<map-number>/sub_issues -F sub_issue_id=<child-database-id>
  ```

  The database ID is `.id`, not the issue number or `node_id`. These REST commands work without the newer CLI relationship flags. GitHub's sub-issue order is the map order; preserve it when selecting tickets.

### Blocking

Create all tickets first, then wire dependencies in a second pass. To make a ticket wait for another issue:

```powershell
gh api repos/joss-isbell/codegraph/issues/<blocker-number> --jq .id
gh api --method POST repos/joss-isbell/codegraph/issues/<ticket-number>/dependencies/blocked_by -F issue_id=<blocker-database-id>
```

A ticket is unblocked only when every blocking issue is closed. Query each blocker's current `state` from the paginated dependency endpoint to determine whether all blockers are closed.

### Frontier query

The frontier is the map's open, unassigned children with no open blockers. This PowerShell query fetches every page of children and dependencies, keeps GitHub's child order, and stops if a read fails. Set `$mapNumber` to the map's issue number; the first emitted ticket is the next frontier ticket. The query reads current state through separate requests; assignment below claims the ticket.

```powershell
$mapNumber = 123
$childNumbers = @(gh api --paginate "repos/joss-isbell/codegraph/issues/$mapNumber/sub_issues?per_page=100" --jq '.[].number')
if ($LASTEXITCODE -ne 0) { throw 'Could not read the map children.' }

foreach ($childNumber in $childNumbers) {
    $ticketJson = gh issue view $childNumber --repo joss-isbell/codegraph --json number,title,state,assignees,url
    if ($LASTEXITCODE -ne 0) { throw "Could not read ticket $childNumber." }
    $ticket = ($ticketJson -join [Environment]::NewLine) | ConvertFrom-Json
    if ($ticket.state -ne 'OPEN' -or @($ticket.assignees).Count -gt 0) { continue }

    $blockerStates = @(gh api --paginate "repos/joss-isbell/codegraph/issues/$childNumber/dependencies/blocked_by?per_page=100" --jq '.[].state')
    if ($LASTEXITCODE -ne 0) { throw "Could not read blockers for ticket $childNumber." }
    $openBlockers = @($blockerStates | Where-Object { $_ -ne 'closed' })
    if ($openBlockers.Count -eq 0) {
        [pscustomobject]@{ number = $ticket.number; title = $ticket.title; url = $ticket.url }
    }
}
```

### Claim and resolve

- **Claim first**: `gh issue edit <ticket-number> --repo joss-isbell/codegraph --add-assignee joss-isbell`. Assignment to the dev driving the map is the claim; skip assigned tickets. `--add-assignee "@me"` is equivalent when authenticated as `joss-isbell`.
- **Record the answer**: `gh issue comment <ticket-number> --repo joss-isbell/codegraph --body-file resolution.md`.
- **Close the ticket**: `gh issue close <ticket-number> --repo joss-isbell/codegraph`.
- **Update the map**: fetch its current body, append one context pointer under **Decisions so far** in the form `- [<ticket title>](<ticket URL>): <one-line gist>`, then save the updated body and use `gh issue edit <map-number> --repo joss-isbell/codegraph --body-file map-body.md`. The answer remains on the ticket. Preserve other sessions' current map edits.

Refer to maps and tickets by their linked titles in human-facing text. Follow the Wayfinder skill for resolving ticket types, surfacing new tickets, and updating the map's fog or scope.
