from fina_core.process_scheduler import Event, SchedulerService
from fina_core.integrations import register_fina_handlers


def test_trade_event_triggers_registered_pricer_and_olap_handlers():
    service = SchedulerService()
    calls = []
    class Trades:
        def __init__(self): self.trades = {}
        def register(self, trade): self.trades[trade['trade_id']] = trade; return trade
        def amend(self, trade_id, changes, reason):
            self.trades[trade_id].update(changes); self.trades[trade_id]['status'] = 'AMENDED'
            event = {'topic': 'trade.lifecycle.amended', 'payload': {'trade_id': trade_id, 'reason': reason}}
            service.bus.publish(Event(event['topic'], event['payload']))
            return self.trades[trade_id]
    trades = Trades()
    def price(request): calls.append(request); return {'PV': 100 if len(calls) == 1 else 97, 'RiskCube': {'cells': [{'sensitivities': {'delta': -0.3 if len(calls) == 1 else -0.4, 'vega': 0.1}}]}}
    def olap(payload): return {'avg_delta': payload['latest_reprice']['quote']['RiskCube']['cells'][0]['sensitivities']['delta']}
    register_fina_handlers(service, pricing_callable=price, trade_repository=trades, olap_callable=olap)
    definition = {
        'metadata': {'name': 'wired'},
        'parameters': {'pricing_request': {'InstrumentKey': {'name': 'FCN'}}},
        'threads': [
            {'name': 'quote', 'handler': 'fina-pricer.pricing_and_sensitivity', 'parameters': {'trade_id': 'T1'}},
            {'name': 'register_trade', 'handler': 'fina-trade.register', 'depends_on': ['quote'], 'parameters': {'trade': {'trade_id': 'T1', 'instrument_id': 'I1', 'product_type': 'FCN', 'notional': 100, 'currency': 'USD'}}},
            {'name': 'amend', 'handler': 'fina-trade.amend', 'depends_on': ['register_trade'], 'parameters': {'trade_id': 'T1', 'changes': {'observation': '2027-06-01'}}},
            {'name': 'reprice', 'handler': 'fina-pricer.pricing_and_sensitivity', 'triggered_by': 'trade.lifecycle.amended', 'parameters': {'trade_id': 'T1'}},
            {'name': 'olap', 'handler': 'fina-olap.group_sensitivities', 'depends_on': ['reprice']},
        ],
        'subscriptions': [{'topic': 'trade.lifecycle.amended', 'handler': 'fina-pricer.pricing_and_sensitivity', 'start_thread': 'reprice'}],
    }
    process = service.create_process(definition)
    assert process.threads[process.id + '/quote'].state == 'FINISHED'
    assert process.threads[process.id + '/reprice'].state == 'FINISHED'
    assert process.threads[process.id + '/olap'].result['avg_delta'] == -0.4
    assert len(calls) == 2
