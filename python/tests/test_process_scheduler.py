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
