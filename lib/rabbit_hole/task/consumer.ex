defmodule RabbitHole.Task.Consumer do
  @moduledoc """
  Simple task consumer API
  """

  use GenServer
  require Logger
  alias RabbitHole.Protocol.{Connection, Channel, Queue, Basic}
  alias RabbitHole.Task

  @type consumer_ref :: any()
  @type processor :: (Task.t(), consumer_ref -> :ok | :error)
  @type opts() :: [processor: processor()]

  @spec start(Exchange.t(), Task.kind(), opts()) :: {:ok, consumer_ref()}
  def start(task_exchange, task_kind, processor: processor) do
    GenServer.start_link __MODULE__, %{te: task_exchange, tk: task_kind, processor: processor}
  end

  def init(%{te: task_exchange, tk: task_kind, processor: processor}) do
    {:ok, conn} = Connection.open
    {:ok, chan} = Channel.open conn
    {:ok, queue} = Queue.declare chan
    routing_key = Task.topic task_kind
    :ok = Queue.bind chan, queue, task_exchange, routing_key: routing_key
    {:ok, consumer_tag} = Basic.consume chan, queue
    state = %{
      conn: conn,
      chan: chan,
      consumer_tag: consumer_tag,
      processor: processor
    }
    {:ok, state}
  end

  @spec stop(consumer_ref()) :: :ok
  def stop(ref) do
    GenServer.stop ref
  end

  def terminate(_reason, %{conn: conn, chan: chan, consumer_tag: consumer_tag}) do
    {:ok, _} = Basic.cancel chan, consumer_tag
    :ok = Channel.close chan
    :ok = Connection.close conn
  end

  def handle_info({:basic_deliver, message, _}, state) do
    message
      |> Task.from_message()
      |> state.processor.(self())

    {:noreply, state}
  end

  def handle_info(msg, state) do
    IO.inspect msg
    IO.inspect state
    {:noreply, state}
  end
end
