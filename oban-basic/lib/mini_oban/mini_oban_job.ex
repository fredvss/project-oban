defmodule MiniOban.Job do
  defstruct [
    :id,
    :type,
    :payload,
    :inserted_at,
    :started_at,
    :finished_at,
    attempts: 0,
    max_attempts: 3,
    status: :pending # :pending | :running | :success | :failed
  ]
end
