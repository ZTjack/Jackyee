import asyncio
import logging
import math
import random
import string
from typing import Dict

import arrow
import qbtrade as qb
import qbxt
import yaml
from docopt import docopt as docoptinit

class Order:
    def __init__(self):
        self.eoid = None # exchange_id
        self.coid = None # client_id
        self.status = None
        self.bs = None
        self.entrust_time = None  # 本地下单的arrow.now().float_timestamp
        self.entrust_price = None
        self.entrust_amount = None
        self.dealt_amt = 0
        self.first_cancel_time = None
        self.last_cancel_time = None
        self.opt = {}
        self.cancel_times = 0
        self.extra = None

class Config:
    def __init__(self):
        self.price_rule = 'market'  # limit: bid被bid1约束；market: bid被ask1约束；taker: 无视盘口。
        self.maker_return = 1.0  # 作为maker的回报率。当手续费为0.1%，回报率为0.999；当享受0.05% rebate时，回报率1.0005。
        self.taker_return = 1.0  # 同上
        self.level_count = 3  # 挂出去3档
        self.amt = None  # 最多持有仓位
        self.diff = None  # e.g. 1.01 整个diff 由 diff 定
        self.earn = None  # e.g. 1.005
        self.trade = True
        self.force_maker = True
        self.tick_delay_seconds = 3
        self.trade_interval = 3

        self.stable_seconds = 3  # do action 需要持续x秒才真的do action

        self.middle = 1
        self.place_amt = 1

        self.acc = ''
        self.coin = ''
        self.c1_symbol = ''
        self.c2_symbol = ''
        self.cooldown_seconds = 60  # 60s
        self.max_pending_orders = 2
        self.open_close_threshold = 5
        self.cancel_order_interval = 1
        self.max_cancel_times = 5
        self.base_amt = None

    def set_key(self, k):
        self.key = k

    async def get_config_dict(self):
        conn = await qb.util.get_async_redis_conn()
        yml = await conn.get(self.key + '.yml')
        if yml:
            return yaml.load(yml)
        else:
            qb.panic(f'no config found {self.key}')

    async def sync(self, display=False):
        dct = await self.get_config_dict()
        if display:
            logging.info('load', dct)
        self.load_dict(dct)

    def load_dict(self, dct):
        if not dct:
            return
        for k, v in dct.items():
            orig = getattr(self, k)
            if orig != v:
                logging.info('config change', k, v)
            setattr(self, k, v)

# 工具类
class Timer(object):
    def __init__(self, action, tags=None):
        if tags:
            self.tags = tags
        else:
            self.tags = {}
        self.tags['action'] = action

    def __enter__(self):
        self.start = arrow.now()

    def __exit__(self, *args, **kwargs):
        self.end = arrow.now()
        key = 'elapse'
        diff = (self.end - self.start).total_seconds()
        # timerInflux.add_point(key=key, tags=self.tags, fields={'value': diff})

class Strategy:
    def __init__(self,stid:str):
        self.acc_symbol = ''
        self.cons = []
        self.acc: qbxt.Account

        self.config = Config()
        self.con_basics: Dict[str, qb.Contract] = {}
        self.stid = stid
        self.inited = False

        self.pos_by_ws = {}
        self.pos_by_rest = {}

        self.asset_by_ws = {}
        self.asset_by_rest = {}

        self.ticks: Dict[str, qbxt.model.OrderbookUpdate] = {}
        self.bbo: Dict[str, qbxt.model.BboUpdate] = {}
        self.bbo_update_q = asyncio.Queue(1)

        self.rc_until = arrow.now()
        self.last_trade_time = 0
        self.last_cancel_all_time = arrow.now()

        self.active_orders: Dict[str, Order] = {}
        self.abnormal_orders: Dict[str, Order] = {}

        self.dealt_info: Dict[str, float] = {}

    @property
    def c1(self):
        return self.cons[0]

    @property
    def c2(self):
        return self.cons[1]

    @property
    def tk1(self) -> qbxt.model.BboUpdate:
        return self.bbo[self.c1]

    @property
    def tk2(self) -> qbxt.model.OrderbookUpdate:
        return self.ticks[self.c2]

    @property
    def pos1(self):
        return self.pos_by_ws[self.c1]['total_amount']

    @property
    def pos2(self):
        return self.pos_by_ws[self.c2]['total_amount']

    @property
    def rc_working(self):
        return arrow.now() < self.rc_until

    @property
    def min_change(self, c):
        return self.con_basics[c].min_change

    @property
    def min_amount(self, c):
        return self.con_basics[c].min_amount

    @staticmethod
    def rounding(value, unit, func=round):
        fmt = '{:.' + str(len(f'{unit:.10f}'.rstrip('0'))) + 'f}'
        res = unit * func(round(value / unit, 4))
        return float(fmt.format(res))

    @classmethod
    def statsd_tags(cls, tags):
        return ['{}:{}'.format(k, v) for k, v in tags.items()]

    def gauge(self, key: str, value, tags=None, ts=None):
        assert self.stid
        assert tags is None or isinstance(tags, dict)
        # key = 'strategy/{}/{}'.format(self.stid, key)
        if tags is None:
            tags = {}
        if isinstance(value, dict):
            print('gauge')
            # self.influxdbudp.add_point(key, value, tags, ts=ts)
        else:
            print('not gauge')
            # self.influxdbudp.add_point(key, {'value': value}, tags, ts=ts)

    def update_bbo(self, bbo:qbxt.model.BboUpdate):
        if bbo.bid1 is None:
            logging.warning('bid1 none', bbo.contract, bbo.bid1, bbo.ask1)
            return
        if bbo.ask1 is None:
            logging.warning('ask1 none', bbo.contract, bbo.bid1, bbo.ask1)
            return
        last_bbo = self.bbo.get(bbo.contract, None)
        if last_bbo:
            now = arrow.now().float_timestamp
            self.gauge('tk1-interval', (now - self.tk1.recv_time / 1e3) * 1e3)
        self.bbo[bbo.contract] = bbo

        now = arrow.now().float_timestamp
        self.gauge('tk1-delay', (now -self.tk1.exg_time / 1e3) * 1e3)

    def update_tick(self, tk:qbxt.model.OrderbookUpdate):
        if tk.bid1 is None:
            logging.warning('bid1 none', tk.contract, tk.bid1, tk.ask1)
            return
        if tk.ask1 is None:
            logging.warning('ask1 none', tk.contract, tk.bid1, tk.ask1)
            return
        last_tk = self.ticks.get(tk.contract, None)
        if last_tk:
            now = arrow.now().float_timestamp
            self.gauge('tk-interval', (now - self.tk2.recv_time / 1e3) * 1e3)
        self.ticks[tk.contract] = tk

        now = arrow.now().float_timestamp
        self.gauge('tk2-delay', (now - self.tk2.recv_time / 1e3) * 1e3)
        if now - self.tk2.recv_time / 1e3 > self.config.tick_delay_seconds:
            self.rc_trigger(10, 'tk2-delay')

        #????
        if not self.bbo_update_q.empty():
            self.bbo_update_q.get_nowait()
        self.bbo_update_q.put_nowait('update_bbo')

    def asset_callback(self, asset: qbxt.model.Assets):
        for data in asset.data['assets']:
            self.asset_by_ws[data['currency']] = data
        return

    def position_callback(self, pos:qbxt.model.WSPositionUpdate):
        for data in pos.data['positions']:
            self.pos_by_ws[data['contract']] = data
        return

    def get_options(self, con, bs, amount, force_maker=False):
        pos = self.pos_by_ws[con]['detail']
        opt = {}
        open_close = 'open'
        if force_maker:
            opt['force_maker'] = True
        if bs == 'b':
            if float(pos['short_avail_qty']) > amount * self.config.open_close_threshold:
                open_close = 'close'
        if bs == 's':
            if float(pos['long_avail_qty']) > amount * self.config.open_close_threshold:
                open_close = 'close'
        opt[open_close] = True
        return opt

    def new_coid(self, con, bs):
        return con + '-' + f'{bs}ooooo' + ''.join(random.choice(string.ascii_letters, k=10))

    async def place_hedge_order(self, bs, amt, c1_dealt_price=None):
        opt = self.get_options(self.c2, bs=bs, amount=amt, force_maker=False)
        coid = self.new_coid(self.c2, bs)
        if c1_dealt_price:
            self.dealt_info[coid] = c1_dealt_price
        if bs == 'b':
            with Timer('place-taker-order'):
                res, err = await self.acc.place_order(self.c2, self.tk2.ask1 * 1.01, bs, amt, client_oid=coid, options=opt)

            if err:
                logging.warning(err, 'place-taker-b')
                self.rc_trigger(self.config.cooldown_seconds, 'place-taker')
        elif bs == 's':
            with Timer('place-taker-order'):
                res, err = await self.acc.place_order(self.c2, self.tk2.bid1 * 0.99, bs, amt, client_oid=coid, options=opt)
            if err:
                logging.warning(err, 'place-taker-s')
                self.rc_trigger(self.config.cooldown_seconds, 'place-taker')

    def gauge_order_extra(self, o:Order):
        field = {
            'tk1-bid1-place': o.extra.tk1_bbo_place.bid1, 'tk1-ask1-place': o.extra.tk1_bbo_place.ask1,
            'tk2-bid1-place': o.extra.tk2_bbo_place.bid1, 'tk2-ask1-place': o.extra.tk2_bbo_place.ask1,
            'tk1-bid1-dealt': self.tk1.bid1, 'tk1-ask1-dealt': self.tk1.ask1,
            'tk2-bid1-dealt': self.tk2.bid1, 'tk2-ask1-dealt': self.tk2.ask1,
        }
        if o.bs == 'b':
            field['diff-b-place'] = o.entrust_price / o.extra.tk2_bbo_place.bid1,
            field['diff-b-dealt'] = o.entrust_price / self.tk2.bid1
        elif o.bs == 's':
            field['diff-s-place'] = o.entrust_price / o.extra.tk2_bbo_place.ask1
            field['diff-s-dealt'] = o.entrust_price / self.tk2.ask1

        if o.first_cancel_time:
            self.gauge('order-cancel-but-dealt', 1, ts=o.first_cancel_time)
        self.gauge('order-extra', field)

    def order_callback(self, orig: qbxt.model.OrderUpdate):
        print('order_callback')
        order = orig.order
        if order.contract == self.c1:
            order_in_memory = self.active_orders.get(order.client_oid, None)
            if not order_in_memory:
                logging.warning(f'{order.client_oid}, {order.exchange_oid} not in active orders')
                return
            now = arrow.now().float_timestamp
            elapse = now - order_in_memory.entrust_time
            if order_in_memory.first_cancel_time:
                elapse = now - order_in_memory.first_cancel_time
            self.gauge('order-elapse', elapse, tags={'status': order.status})
        if order.status == qbxt.model.Order.PENDING and order.contract == self.c1:
            return

        if 'deal' in order.status and order.contrac == self.c1:
            order_in_memory = self.active_orders.get(order.client_oid, None)
            if not order_in_memory:
                logging.warning(f'${order.client_oid}, {order.exchange_oid} not in active orders')
                return
            amt = order.dealt_amount - order_in_memory.dealt_amt
            self.active_orders[order.client_oid].dealt_amt = order.dealt_amount
            self.active_orders[order.client_oid].extra.tk1_bbo_dealt = self.tk1
            self.active_orders[order.client_oid].extra.tk2_bbo_dealt = self.tk2

            if amt > 0:
                bs = 'b' if order.bs == 's' else 's'
                qb.fut(self.place_hedge_order(bs, amt, order.average_dealt_price))

                self.gauge('place-price', order.entrust_price, {'bs': order.bs}, ts=order_in_memory.entrust_time)
                self.gauge('dealt-amt', amt, tags={'bs': order.bs})
                self.gauge_order_extra(self.active_orders[order.client_oid])
        if order.status == qbxt.model.Order.DEALT and order.contract == self.c2:
            c1_dealt_price = self.dealt_info.get(order.client_oid, None)
            if c1_dealt_price:
                self.gauge('dealt-diff', c1_dealt_price / order.average_dealt_price, tags={'bs': qb.util.op_bs(order.bs)})
                self.dealt_info.pop(order.client_oid, None)

        if 'deal' in order.status:
            con = 'tick1' if order.contract == self.c1 else 'tick2'
            self.gauge('dealt', order.average_dealt_price, tags={'bs': order.bs, 'con_symbol': order.contract, 'con': con})

        if order.status in qbxt.model.Order.END_SET and order.contract == self.c1:
            self.active_orders.pop(order.client_oid, None)
        return


    def rc_trigger(self, second, reason):
        if not self.rc_working:
            qb.fut(self.cancel_all())
            logging.warning('rc trigger', reason, second, self.rc_until)
            self.gauge('rc-work', 1, {'type': reason})
        self.rc_until = max(self.rc_until, arrow.now().shift(seconds=second))

    async def cancel_all(self):
        print('cancel_all')

    async def init(self):
        self.config.get_key(f"strategy:{self.stid}:config")
        await self.config.sync()
        self.acc_symbol = self.config.acc
        self.cons = [self.config.c1_symbol, self.config.c2_symbol]

        logging.info(self.c1, self.c2)

        self.quote1 = await qbxt.new_quote('okef', interest_cons=[self.c1], use_proxy=True, bbo_callback=self.update_bbo)

        self.quote2 = await qbxt.new_quote('okef', interest_cons=[self.c2], use_proxy=True, orderbook_callback=self.update_tick)

        for con in self.cons:
            self.con_basics[con] = qb.con(con)

        await asyncio.sleep(1)
        logging.info('quote init')

        logging.info(f'using account {self.acc_symbol}')
        self.acc = await qbxt.new_account(self.acc_symbol, use_1token_auth=True, use_proxy=True, interest_cons=[self.c1, self.c2], asset_callback=self.asset_callback, position_callback=self.position_callback, order_callback=self.order_callback)





async def main():
    print('main')
    # s = Strategy(stid=stid)
    # await s.run()


async def play():
    print('play demo')


if __name__ == '__main__':
    qb.init(env='prod')
    docopt = docoptinit(__doc__)
    stid = str(docopt['--stid'])
    logging.info(f'stid: {stid}')
    if docopt['--play']:
        qb.fut(play())
    else:
        qb.fut(main())
    qb.run_forever()