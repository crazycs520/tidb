package interval

type JobState = string

const (
	JobStateNone         JobState = "none"
	JobStateMarkReadonly JobState = "mark_read_only"
	JobStateMovingData   JobState = "moving_data"
	JobStateMovingDone   JobState = "move_data_done"
	JobStateDone         JobState = "done"
	JobStateCancelled    JobState = "canceled"
)
