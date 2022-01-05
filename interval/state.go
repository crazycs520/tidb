package interval

type JobState = string

const (
	JobStateNone            JobState = "none"
	JobStateMovingData      JobState = "moving_data"
	JobStateUpdateTableMeta JobState = "update_table_meta"
	JobStateDone            JobState = "done"
	JobStateCancelled       JobState = "canceled"
)
