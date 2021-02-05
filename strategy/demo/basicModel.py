import qbtrade as qb
import yaml
import logging
import qbxt
import arrow
import time
import random
import string
import socket
from typing import Dict
import asyncio


class Extra:
    def __init__(self):
        self.tk1_bbo_place = None
        self.tk2_bbo_place = None

        self.tk1_bbo_dealt = None
        self.tk2_bbo_dealt = None


class Order:
    def __init__(self):
        self.eoid = None
        self.coid = None
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


# for gauge start


class InfluxdbGeneralUDP:
    def __init__(self, host=None, port=None, auto_refresh_interval=0.1):

        if host is None and port is None:
            if qb.config.region == 'awstk':
                host = 'awstk-db-0.machine'
                port = 8089
            else:
                host = 'alihk.influxdb.qbtrade.org'
                port = 8090
        self.host = host
        self.port = port
        self._ls = []
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.connect((host, port))
        qb.fut(self.auto_flush(auto_refresh_interval))

    def add_point(self, measurement, fields, tags, ts=None):
        """

        :param measurement:
        :param fields:
        :param tags:
        :param ts: time.time() in second
        :return:
        """

        if ts is None:
            ts = time.time()
        ts = int(ts * 1e9)
        line = self.get_line(measurement, fields, tags, ts)
        if line:
            self._ls.append(line)

    @staticmethod
    def get_line(measurement, fields, tags, ts_ns):
        if tags:
            tag_line = ''.join(
                [f',{key}={value}' for key, value in tags.items()])
        else:
            tag_line = ''
        if not fields:
            # print('ignore', fields)
            return ''

        fields_list = []
        for k, v in fields.items():
            if isinstance(v, int):
                fields_list.append(f'{k}={float(v)}')
            if isinstance(v, float):
                fields_list.append(f'{k}={v}')
        if not fields_list:
            return ''
        fields_str = ','.join(fields_list)
        return f'{measurement}{tag_line} {fields_str} {ts_ns}'

    def flush(self):  # udp by design就不是用来做batch的，可用长度太小。
        for line in self._ls:
            self.sock.send(line.encode('utf8'))
        self._ls.clear()

    async def auto_flush(self, interval):
        while True:
            try:
                await asyncio.sleep(interval)
            except RuntimeError:
                print('event loop is closed')
                break
            try:
                a = time.time()
                length = len(self._ls)
                self.flush()
                b = time.time() - a
                if b * 1000 > 3:  # warn if > 3ms rquired
                    ms = round(b * 1000, 2)
                    print(f'long flush use {ms}ms', length)
            except Exception:
                logging.exception('unexpected')
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                self.sock.connect((self.host, self.port))


influx_udp_client = InfluxdbGeneralUDP()


class InfluxdbUdpGauge:
    def __init__(self, stid, host=None, port=None, measurement_prefix=False):
        if host is None and port is None:
            if qb.config.region == 'awstk':
                host = 'awstk-db-0.machine'
                port = 8089
            else:
                host = 'alihk.influxdb.qbtrade.org'
                port = 8090

        self.host = host
        self.port = port
        self._ls = []
        if measurement_prefix:
            self.measurement = f'strategy/{stid}'
        else:
            self.measurement = f'{stid}'
        # self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # self.sock.connect((host, port))
        # if auto_flush_interval > 0:
        #     qb.fut(self.auto_flush(auto_flush_interval))

    def add_point(self, key, fields, tags, ts=None):
        """

        :param key:
        :param fields:
        :param tags:
        :param ts:  time.time() in second
        :return:
        """

        influx_udp_client.add_point(self.measurement + '/' + key, fields, tags,
                                    ts)

    # def get_line(self, key, fields, tags, ts):
    #     if tags:
    #         tag_line = ''.join([f',{key}={value}' for key, value in tags.items()]) # noqa
    #     else:
    #         tag_line = ''
    #     if not fields:
    #         # print('ignore', fields)
    #         return ''


# for gauge  end
class Config:
    def __init__(self):
        self.account_symbol = 'huobip/subdjw8'
        self.contract1 = 'huobip/link.usdt'
        self.contract2 = 'huobip/bch.usdt'
        self.exchange = 'huobip'
        # 假如设置为False，账户不会下单
        self.trade = True
        # 一般是本位币的配置，可以gauge这个算balance
        self.base_coin = 'usdt'
        # 策略本位币数量
        self.base_coin_amt = 0

        # 允许最多的pending-order数量
        self.max_pending_orders: 5
        # 系统触发风控，系统会出于风控状态的默认时间
        self.cooldown_seconds = 10
        # 一个单子允许撤销几次
        self.cancel_order_interval = 1
        # 一个单子允许撤销的次数
        self.max_cancel_times = 3
        # TODO
        self.trade_interval = 3
        # TODO
        self.open_close_threshold = 5

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
            # pylint: disable=logging-too-many-args
            logging.info('load', dct)
        self.load_dict(dct)

    def load_dict(self, dct):
        if not dct:
            return
        for k, v in dct.items():
            orig = getattr(self, k)
            if orig != v:
                # pylint: disable=logging-too-many-args
                logging.info('config change', k, v)
            setattr(self, k, v)


class Strategy:
    def __init__(self, s_tid: str):
        self.s_tid = s_tid
        self.inited = False
        self.config = Config()
        # gauge数据用到
        self.influxDb_udp = InfluxdbUdpGauge(self.s_tid,
                                             measurement_prefix=True)
        # 账户id
        self.account_symbol = None
        # 交易标的
        self.contracts = []
        # contract设置参数
        self.con_basics: Dict[str, qb.Contract] = {}
        # 存Tick的地方
        self.bbo: Dict[str, qbxt.model.BboUpdate] = {}
        # tick数据Queue
        self.bbo_update_q = asyncio.Queue(1)

        self.acc: qbxt.Account
        # 存放ws推送的资产
        self.asset_by_ws = {}
        # rest存放资产
        self.asset_by_rest = {}
        # 存放ws推送的持仓
        self.pos_by_ws = {}
        # 存放rest获取的持仓数据
        self.pos_by_rest = {}

        # 前端自己维护的ActiveOrders
        self.active_orders: Dict[str, Order] = {}
        # 存下单时候的价格，用来和实际ws推送的成交价格计算滑点
        self.dealt_info: Dict[str, float] = {}

        # noqa 风控参数，假如触发分控，这个数值会被赋值，没到这个时间前为风控中状态
        self.rc_until = arrow.now()
        # 上次cancel_all的时间
        self.last_cancel_all_time = arrow.now()
        # 上次order时间，风控用
        self.last_trade_time = 0

    @property
    def contract1(self):
        return self.contracts[0]

    @property
    def contract2(self):
        return self.contracts[1]

    @property
    def tk1(self) -> qbxt.model.BboUpdate:
        return self.bbo[self.contract1]

    @property
    def tk2(self) -> qbxt.model.BboUpdate:
        return self.bbo[self.contract2]

    @property
    def rc_working(self):
        return arrow.now() < self.rc_until

    def gauge(self, key: str, value, tags=None, ts=None):
        assert self.s_tid
        assert tags is None or isinstance(tags, dict)
        # key = 'strategy/{}/{}'.format(self.stid, key)
        if tags is None:
            tags = {}
        if isinstance(value, dict):
            self.influxDb_udp.add_point(key, value, tags, ts=ts)
        else:
            self.influxDb_udp.add_point(key, {'value': value}, tags, ts=ts)

    async def update_bbo(self, bbo: qbxt.model.BboUpdate):
        if bbo.bid1 is None:
            # pylint: disable=logging-too-many-args
            logging.warning('bid1 none', bbo.contract, bbo.bid1, bbo.ask1)
            return
        if bbo.ask1 is None:
            # pylint: disable=logging-too-many-args
            logging.warning('ask1 none', bbo.contract, bbo.bid1, bbo.ask1)
            return
        last_bbo = self.bbo.get(bbo.contract, None)

        if last_bbo:
            now = arrow.now().float_timestamp
            last_recv_time = self.tk1.recv_time if bbo.contract == self.contract1 else self.tk2.recv_time  # noqa
            last_exg_time = self.tk1.exg_time if bbo.contract == self.contract1 else self.tk2.exg_time  # noqa
            self.gauge(f'{bbo.contract}-interval',
                       (now - last_recv_time / 1e3) * 1e3)
            self.gauge(f'{bbo.contract}-delay',
                       (now - last_exg_time / 1e3) * 1e3)
        self.bbo[bbo.contract] = bbo

        # 假如有的tick推送特别频繁，queue里面的东西来不及处理，手动清空一下
        if bbo.contract == self.contract2:
            if not self.bbo_update_q.empty():
                self.bbo_update_q.get_nowait()
            self.bbo_update_q.put_nowait('update_bbo')

    async def asset_callback(self, asset: qbxt.model.Assets):
        for data in asset.data['assets']:
            self.asset_by_ws[data['currency']] = data
        return

    async def position_callback(self, pos: qbxt.model.WSPositionUpdate):
        for data in pos.data['positions']:
            self.pos_by_ws[data['contract']] = data
        return

    async def order_callback(self, orig: qbxt.model.OrderUpdate):
        order = orig.order
        if order.contract == self.contract1:
            # 假如推送非本地维护的orderList的单子，说明程序出问题
            order_in_memory = self.active_orders.get(order.client_oid, None)
            if not order_in_memory:
                logging.warning(
                    f'{order.client_oid}, {order.exchange_oid} not in active orders'  # noqa
                )
                return
            now = arrow.now().float_timestamp
            # 记录一下订单时间差值
            elapse = now - order_in_memory.entrust_time
            if order_in_memory.first_cancel_time:
                elapse = now - order_in_memory.first_cancel_time
            self.gauge('order-elapse', elapse, tags={'status': order.status})
        # 对于pending订单不做处理
        if order.status == qbxt.model.Order.PENDING and order.contract == self.contract1:  # noqa
            return
        # 假如有成交结果了，即使部分成交
        if 'deal' in order.status and order.contract == self.contract1:
            order_in_memory = self.active_orders.get(order.client_oid, None)
            if not order_in_memory:
                logging.warning(
                    f'{order.client_oid}, {order.exchange_oid} not in active orders'  # noqa
                )
                return
            self.active_orders[order.client_oid].dealt_amt = order.dealt_amount
            # ???????
            self.active_orders[order.client_oid].extra.tk1_bbo_dealt = self.tk1
            self.active_orders[order.client_oid].extra.tk2_bbo_dealt = self.tk2
            # strategy related

        if order.status == qbxt.model.Order.DEALT and order.contract == self.contract1:  # noqa
            c1_dealt_price = self.dealt_info.get(order.client_oid, None)
            if c1_dealt_price:
                self.gauge('dealt-diff',
                           c1_dealt_price / order.average_dealt_price,
                           tags={'bs': qb.util.op_bs(order.bs)})
                self.dealt_info.pop(order.client_oid, None)
        # Gauge dealt info
        if 'deal' in order.status:
            self.gauge('dealt',
                       order.average_dealt_price,
                       tags={
                           'bs': order.bs,
                           'con_symbol': order.contract
                       })
        # 假如结束状态了，更新本地的ActiveOrders
        if order.status in qbxt.model.Order.END_SET and order.contract == self.contract1:  # noqa
            self.active_orders.pop(order.client_oid, None)
        return

    async def init(self):
        self.config.set_key(f"strategy:{self.s_tid}:config")
        await self.config.sync()
        self.account_symbol = self.config.account_symbol
        self.contracts = [self.config.contract1, self.config.contract2]
        # 订阅行情
        self.quote = await qbxt.new_quote(
            self.config.exchange,
            interest_cons=[self.contract1, self.contract2],  # TODO 看具体情况修改
            use_proxy=True,
            bbo_callback=self.update_bbo)
        for contract in self.contracts:
            self.con_basics[contract] = qb.con(contract)
        await asyncio.sleep(1)
        # 等tick数据都OK
        while len(self.bbo) != len(self.contracts):
            await asyncio.sleep(0.1)
        logging.info('quote init complete')

        self.acc = await qbxt.new_account(
            self.account_symbol,
            use_1token_auth=True,
            use_proxy=True,
            interest_cons=[self.contract1, self.contract2],
            asset_callback=self.asset_callback,
            position_callback=self.position_callback,
            order_callback=self.order_callback)
        await asyncio.sleep(1)
        logging.info('account init complete')

    # 相对低频的通过rest来完成的事情，比如说gauge收益，比较弱的风控
    async def update_info(self):
        len_active_orders = len(self.active_orders)
        self.gauge('active-orders', len_active_orders)
        # 假如委托数量多于程序最多数量
        if len_active_orders > self.config.max_pending_orders:
            # 处理30分钟前的单子
            over_due_time = arrow.now().shift(minutes=-30).float_timestamp
            for coid, o in self.active_orders.items():
                if o.entrust_time < over_due_time:
                    logging.warning(
                        f'delete old order {o.bs}, {o.entrust_price}, {o.coid}, {o.eoid}'  # noqa
                    )
                    self.active_orders.pop(coid, None)
        asset, err = await self.acc.get_assets(contract=self.contract1)
        if not err:
            for data in asset.data['assets']:
                self.asset_by_rest[data['currency']] = data
            try:
                total_amount = float(
                    self.asset_by_rest[self.config.base_coin]['total_amount'])
            except Exception:
                total_amount = 0
            self.gauge('coin-amount', total_amount)
            if self.config.base_coin_amt and self.config.base_coin_amt > 0:
                self.gauge('profit-rate',
                           total_amount / self.config.base_coin_amt)
        else:
            logging.warning('get assets error')
            self.rc_trigger(self.config.cooldown_seconds, 'get-assets')

        await self.update_position()

    # 通过rest获取position参数，主要低频风控和收集信息
    async def update_position(self):
        pos1, err1 = await self.acc.get_positions(self.contract1, None)
        if not err1:
            for data in pos1.data['positions']:
                self.pos_by_rest[data['contract']] = data
            try:
                pos1_amt = float(
                    self.pos_by_rest[self.contract1]['total_amount'])
            except Exception:
                pos1_amt = 0
            self.gauge("pos1_amount", pos1_amt)

        else:
            logging.warning('get position1 error')
            self.rc_trigger(self.config.cooldown_seconds, 'get-position1')

        pos2, err2 = await self.acc.get_positions(self.contract2, None)
        if not err2:
            for data in pos2.data['positions']:
                self.pos_by_rest[data['contract']] = data
            try:
                pos2_amt = float(
                    self.pos_by_rest[self.contract2]['total_amount'])
            except Exception:
                pos2_amt = 0
            self.gauge("pos2_amount", pos2_amt)
        else:
            logging.warning('get position2 error')
            self.rc_trigger(self.config.cooldown_seconds, 'get-position2')
            return

    async def cancel_all(self):
        now = arrow.now()
        if now.shift(seconds=-10) < self.last_cancel_all_time:
            return
        self.last_cancel_all_time = now
        for contract in self.contracts:
            list, err = await self.acc.get_pending_list(contract=contract)
            if err:
                logging.warning(err)
                return
            for item in list.orders:
                await asyncio.sleep(0.1)
                res, err = await self.acc.cancel_order(item.exchange_oid)
                if err:
                    logging.warning(err)
                    continue

    # 一般套利策略、对冲策略需要实现这个方法
    async def match_pos(self):
        print('match_pos')

    # 假如触发了风控,撤掉所有单子
    def rc_trigger(self, second, reason):
        if not self.rc_working:
            qb.fut(self.cancel_all())
            logging.warning(f'rc trigger {reason} ${second} ${self.rc_until}')
            self.gauge('rc-work', 1, {'type': reason})
        self.rc_until = max(self.rc_until, arrow.now().shift(seconds=second))

    # 撤单方法
    async def cancel_order(self, eoid=None, coid=None):
        if eoid:
            res, err = await self.acc.cancel_order(exchange_oid=eoid)
        elif coid:
            res, err = await self.acc.cancel_order(client_oid=coid)
        if err:
            if err.code not in [qbxt.model.Error.EXG_CANCEL_ORDER_NOT_FOUND]:
                logging.warning(res, err, eoid, coid)

    # TODO 需要请教一波逻辑
    def handle_remain_orders(self, bs, price):
        ideal_order_in_active = False
        for coid, o in list(self.active_orders.items()):
            if bs != o.bs:
                continue
            # 一样的单子，last_cancel_time是None
            if price == o.entrust_price and not o.last_cancel_time:
                ideal_order_in_active = True
            else:
                now = arrow.now().float_timestamp
                if not o.first_cancel_time:
                    o.first_cancel_time = now
                factor = self.active_orders[coid].cancel_time + 1
                if not o.last_cancel_time or now - o.last_cancel_time > self.config.cancel_order_interval * factor:  # noqa
                    if o.eoid:
                        qb.fut(self.cancel_order(eoid=o.eoid))
                    else:
                        qb.fut(self.cancel_order(coid=o.coid))
                    self.active_orders[coid].last_cancel_time = now
                    self.active_orders[coid].cancel_times += 1
                    if self.active_orders[
                            coid].cancel_times > self.config.max_cancel_times:
                        logging.warning(
                            f'{o.coid} {o.eoid} cancel times exceed limit')
                        self.active_orders.pop(o.coid, None)
        return ideal_order_in_active

    # 综合的riskcontrol状态
    async def rc_work(self):
        if not self.inited:
            self.gauge('rc-work', 1, {'type': 'not-inited'})
            return True
        if self.acc.get_ws_state() != qbxt.const.WSState.READY:
            self.gauge('rc-work', 1, {'type': 'acc-ws'})
            return True
        if self.quote.get_ws_state() != qbxt.const.WSState.READY:
            self.gauge('rc-work', 1, {'type': 'quote-ws'})
            return True
        if not self.config.trade:
            self.gauge('rc-work', 1, {'type': 'trade-off'})
            return True
        if self.rc_working:
            return True

    def get_order_options(self, contract, bs, amount, force_maker=False):
        pos = self.pos_by_ws[contract]['detail']
        opt = {}
        open_close = 'open'
        if force_maker:
            opt['force_maker'] = True
        # TODO add comments
        if bs == 'b':
            if float(pos['short_avail_qty']
                     ) > amount * self.config.open_close_threshold:
                open_close = 'close'
        if bs == 's':
            if float(pos['long_avail_qty']
                     ) > amount * self.config.open_close_threshold:
                open_close = 'close'
        opt[open_close] = True
        return opt

    def new_coid(self, con, bs):
        return con + '-' + f'{bs}ooooo' + ''.join(
            random.choices(string.ascii_letters, k=10))

    async def place_maker_order(self, contract, order: Order):
        res, err = await self.acc.place_order(contract,
                                              price=order.entrust_price,
                                              bs=order.bs,
                                              amount=order.entrust_amount,
                                              client_oid=order.coid,
                                              options=order.opt)
        if err:
            self.rc_trigger(self.config.cooldown_seconds, 'place-maker-order')
            return
        if order.coid in self.active_orders.keys():
            self.active_orders[order.coid].eoid = res.exchange_oid

    async def do_action(self, contract: str, bs: str, price: float, amt: float,
                        force_maker: bool):
        ideal_order_in_active = self.handle_remain_orders(bs, price)
        if ideal_order_in_active:
            return
        if len(self.active_orders) > self.config.max_pending_orders:
            return
        now = arrow.now().float_timestamp
        if now - self.last_trade_time < self.config.trade_interval:
            return
        else:
            self.last_trade_time = now

        if await self.rc_work():
            return

        opt = self.get_order_options(contract,
                                     bs,
                                     amt,
                                     force_maker=force_maker)
        coid = self.new_coid(contract, bs)
        order = Order()
        order.coid = coid
        order.entrust_price = price
        order.bs = bs
        order.entrust_amount = amt
        order.entrust_time = arrow.now().float_timestamp
        order.opt = opt
        ext = Extra()
        ext.tk1_bbo_dealt = self.tk1
        ext.tk2_bbo_dealt = self.tk2
        order.extra = ext
        self.active_orders[coid] = order
        qb.fut(self.place_maker_order(contract, order))

    # 这个网格套利相关的逻辑代码在里面
    async def main_callback(self):
        if not self.inited:
            return
        value = {
            'tk1-bid1': self.tk1.bid1,
            'tk1-ask1': self.tk1.ask1,
            'tk2-bid1': self.tk2.bid1,
            'tk2-ask1': self.tk2.ask1,
            'tk-diff': self.tk1.middle / self.tk2.middle,
            'bid-d-ask': self.tk1.bid1 / self.tk2.ask1,
            'ask-d-bid': self.tk1.ask1 / self.tk2.bid1
        }
        # gauge quote data
        self.gauge('quote', value)
        # 策略相关逻辑写到下面去↓↓↓↓↓↓↓↓↓↓↓↓↓↓

        price = 1
        amount = 1
        # 触发下单
        qb.fut(self.do_action(self.contract1, 'b', price, amount, False))

    # 一般策略都是Tick触发，从bbo_update_q里面去拿
    async def daemon_consume_update_bbo(self):
        while True:
            try:
                await self.bbo_update_q.get()
                await self.main_callback()
            except Exception:
                logging.warning('main_callback error')

    async def run(self):
        await self.init()
        await self.update_info()
        # 用rest结果初始化ws数据
        self.asset_by_ws = self.asset_by_rest
        self.pos_by_ws = self.pos_by_rest
        # 先全撤
        await self.cancel_all()
        # 每次拿一下配置，方便
        await self.config.sync()

        self.inited = True

        qb.fut(self.daemon_consume_update_bbo())

        # 低频的风控操作
        qb.fut(qb.autil.loop_call(self.update_info, 30, panic_on_fail=False))
        qb.fut(qb.autil.loop_call(self.config.sync, 30))
        qb.fut(qb.autil.loop_call(self.match_pos, 10))


async def main():
    s_tid = 'st-jack-qbxt-demo'
    s = Strategy(s_tid=s_tid)
    await s.init()
    # await s.update_info()
    # qb.fut(qb.autil.loop_call(s.update_info, 30, panic_on_fail=False))
    # qb.fut(s.do_action('b', 12, s.place_amt, False))


if __name__ == '__main__':
    qb.fut(main())
    qb.run_forever()
