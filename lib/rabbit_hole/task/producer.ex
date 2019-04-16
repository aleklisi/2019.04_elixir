defmodule RabbitHole.Task.Producer do
  @moduledoc """
  Simple task producer API
  """

  use GenServer
  require Logger

  alias RabbitHole.Protocol.{Connection, Channel, Exchange, Basic}
  alias RabbitHole.Task

  @type producer_ref :: any()
  @type exchange :: Exchange.t()
  @type message :: String.t()

  @spec start(Exchange.t()) :: {:ok, producer_ref()}
  def start(ex) do
    GenServer.start_link __MODULE__, ex
  end

  @spec stop(producer_ref()) :: :ok
  def stop(ref) do
    GenServer.stop ref
  end

  def terminate(reason, %{chan: chan, conn: conn}) do
    Logger.info(reason)
    :ok = Channel.close chan
    :ok = Connection.close conn
  end

  @spec publish(producer_ref(), Task.t(), [Basic.publish_opt()]) :: :ok
  def publish(ref, task, opts \\ []) do
    GenServer.cast ref, %{task: task, opts: opts}
  end

  def handle_cast(%{task: task, opts: opts}, state) do
    channel = state.chan
    exchange = state.ex
    routing_key = RabbitHole.Task.topic task
    payload =  RabbitHole.Task.to_message task
    Basic.publish channel, exchange, routing_key, payload, opts
    {:noreply, state}
  end
  def init(ex) do
    {:ok, conn} = Connection.open
    {:ok, chan} = Channel.open conn
    :ok = Exchange.declare chan, ex, :topic
    state = %{conn: conn, chan: chan, ex: ex}
    {:ok, state}
  end
end
