import qbtrade as qb
import yaml
import logging
import qbxt
import arrow
import time
import socket
from typing import Dict
import asyncio


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
        self.account_symbol = 'subdjw8'
        self.contract1 = 'huobip/link.usdt'
        self.contract2 = 'huobip/bch.usdt'
        self.exchange = 'huobip'
        # 一般是本位币的配置，可以gauge这个算balance
        self.base_coin = 'usdt'
        # 策略本位币数量
        self.base_coin_amt = 0

        # 允许最多的pending-order数量
        self.max_pending_orders: 5
        # 系统触发风控，系统会出于风控状态的默认时间
        self.cooldown_seconds = 10

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

    async def get_config_dict(self):
        print('start get redis settings')
        conn = await qb.util.get_async_redis_conn()
        key = f"strategy:{self.s_tid}:config"
        yml = await conn.get(key)
        if yml:
            return yaml.load(yml)
        else:
            qb.panic('no config found')

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
        print('cancel all')

    # 假如触发了风控,撤掉所有单子
    def rc_trigger(self, second, reason):
        if not self.rc_working:
            qb.fut(self.cancel_all())
            logging.warning(f'rc trigger {reason} ${second} ${self.rc_until}')
            self.gauge('rc-work', 1, {'type': reason})
        self.rc_until = max(self.rc_until, arrow.now().shift(seconds=second))


async def main():
    s_tid = 'st-jack-qbxt-demo'
    s = Strategy(s_tid=s_tid)
    await s.get_config_dict()

    # await s.init()
    # await s.update_info()
    # qb.fut(qb.autil.loop_call(s.update_info, 30, panic_on_fail=False))
    # qb.fut(s.do_action('b', 12, s.place_amt, False))


if __name__ == '__main__':
    qb.fut(main())
    qb.run_forever()
