defmodule MiniOban.Job do
  defstruct [
    :id,
    :type,
    :payload,
    attempts: 0,
    max_attempts: 3,
    status: :pending # :pending | :running | :success | :failed
  ]
end
