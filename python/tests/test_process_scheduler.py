from fina_core.process_scheduler import Event, SchedulerService, render_parameters


def test_process_dependencies_and_parameter_rendering():
    seen = []
    service = SchedulerService()

    def quote(thread, runtime):
        runtime.bus.publish(Event("quote.created", {"trade_id": "T1", "pv": 101.25}, thread_id=thread.id))
        return {"trade_id": "T1", "pv": 101.25, "eval": thread.parameters["eval_datetime"]}

    def register(thread, runtime):
        return {"trade_id": "T1", "status": "LIVE"}

    service.register_handler("quote", quote)
    service.register_handler("register", register)
    service.bus.subscribe("quote.created", "assertion", lambda event: seen.append(event.payload))
    definition = {"metadata": {"name": "fcn"}, "parameters": {"eval_datetime": "${eval_datetime}"}, "threads": [
        {"name": "quote", "handler": "quote"},
        {"name": "register", "handler": "register", "depends_on": ["quote"]},
    ]}
    process = service.create_process(render_parameters(definition, {"eval_datetime": "2027-05-18"}))
    assert process.state == "FINISHED"
    assert [t.state for t in process.threads.values()] == ["FINISHED", "FINISHED"]
    assert process.threads[process.id + "/quote"].result["eval"] == "2027-05-18"
    assert seen == [{"trade_id": "T1", "pv": 101.25}]


def test_commands_publish_and_cancel():
    service = SchedulerService()
    received = []
    service.bus.subscribe("trade.lifecycle.amended", "risk", lambda event: received.append(event.payload))
    result = service.command({"command": "publish", "topic": "trade.lifecycle.amended", "payload": {"trade_id": "T1"}})
    assert result["delivered"] == 1
    assert received == [{"trade_id": "T1"}]


def test_priority_orders_ready_threads():
    from fina_core.process_scheduler import SchedulerService

    service = SchedulerService()
    order = []

    def handler(name):
        def run(thread, runtime):
            order.append(name)
            return {"name": name}

        return run

    for name in ("low", "high", "mid"):
        service.register_handler(name, handler(name))
    definition = {
        "metadata": {"name": "prio"},
        "parameters": {},
        "threads": [
            {"name": "low", "handler": "low", "priority": 1},
            {"name": "high", "handler": "high", "priority": 5},
            {"name": "mid", "handler": "mid", "priority": 3},
        ],
    }
    process = service.create_process(definition)
    assert order == ["high", "mid", "low"]  # highest priority scheduled first
    assert process.threads[process.id + "/high"].priority == 5


def test_preempt_command_pauses_lower_priority_threads():
    from fina_core.process_scheduler import ProcessInstance, SchedulerService, ThreadInstance

    service = SchedulerService()
    process = ProcessInstance("p1", "preempt", {})
    process.threads["p1/low"] = ThreadInstance("p1/low", "p1", "low", "noop", {}, priority=1)
    process.threads["p1/high"] = ThreadInstance("p1/high", "p1", "high", "noop", {}, priority=9)
    service.processes["p1"] = process

    result = service.command({"command": "preempt", "process_id": "p1", "priority": 5})
    assert result["paused"] == ["p1/low"]
    assert process.threads["p1/low"].state == "PAUSED"
    assert process.threads["p1/high"].state == "PENDING"
