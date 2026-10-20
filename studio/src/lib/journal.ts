// Plain-language labels for journal events (the timeline is read by analysts, not only engineers).
const LABELS: Record<string, string> = {
  "execution.started": "Started", "execution.completed": "Finished", "execution.faulted": "Failed",
  "execution.cancelled": "Cancelled", "execution.parked": "Paused – waiting for a frozen dependency",
  "execution.waiting": "Waiting for a message or event", "execution.running": "Running again",
  "activity.completed": "Step done", "activity.suspended": "Step waiting", "activity.sleeping": "Step delayed",
  "activity.log": "Note", "work.retry_scheduled": "Will retry", "work.parked": "Step paused",
  "work.resumed": "Step resumed", "work.retried": "Retried after failure", "work.faulted": "Step failed",
  "work.abandoned": "Step abandoned", "bookmark.resumed": "Message/event arrived",
  "intent.delivered": "Message sent", "intent.held": "Message held back", "intent.failed": "Message could not be sent",
  "intent.retry_scheduled": "Message send will retry", "intent.released": "Message released",
  "execution.compensated": "Completed steps were undone", "compensation.started": "Undoing completed steps",
  "compensation.step_completed": "Step undone", "compensation.completed": "Undo finished",
};

export function eventLabel(kind: string): string {
  return LABELS[kind] ?? kind;
}

export const PROPOSAL_LABELS: Record<string, string> = {
  "hold.release": "Unfreeze", "work.resume": "Resume paused step", "work.abandon": "Abandon step",
  "execution.compensate": "Undo a run",
};
