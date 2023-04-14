import asyncio
import datetime
import logging
import os
import signal
import sys
import gzip
from multiprocessing import Process, Queue

from binancefutures import BinanceFutures
from binancefuturescoin import BinanceFuturesCoin
from binance import Binance

queue = Queue()

if sys.argv[1] == 'binancefutures':
    stream = BinanceFutures(queue, sys.argv[2].split(','))
elif sys.argv[1] == 'binance':
    stream = Binance(queue, sys.argv[2].split(','))
elif sys.argv[1] == 'binancefuturescoin':
    stream = BinanceFuturesCoin(queue, sys.argv[2].split(','))
else:
    raise ValueError('unsupported exchange.')


def writer_proc_old(queue, output):
    while True:
        data = queue.get()
        if data is None:
            break
        symbol, timestamp, message = data
        #date = datetime.datetime.fromtimestamp(timestamp).strftime('%Y%m%d')

        dt = datetime.datetime.fromtimestamp(timestamp)
        date = dt.strftime('%Y%m%d')

        # 计算15分钟时间段
        minute_segment = (dt.minute // 15) * 15
        segment_start = dt.replace(minute=minute_segment, second=0, microsecond=0)
        segment_end = segment_start + datetime.timedelta(minutes=15)

        segment_str = segment_start.strftime('%H%M') + '-' + segment_end.strftime('%H%M')

        # filename = os.path.join(output, '%s_%s.dat' % (symbol, date))
        filename = os.path.join(output, f"{symbol}_{date}_{segment_str}.dat")
        with open(filename, 'a') as f:
            f.write(str(int(timestamp * 1000000)))
            f.write(' ')
            f.write(message)
            f.write('\n')


def writer_proc(queue, output):
    file_handles = {}  # 用于存储文件句柄的字典
    last_written_timestamps = {}  # 存储每个文件句柄最后写入的时间戳

    def get_file_handle(file_path, file_name, timestamp):
        # 关闭并删除超过5分钟未写入的文件句柄
        for path, last_written_ts in list(last_written_timestamps.items()):
            if timestamp - last_written_ts > 5 * 60:
                file_handles[path].close()
                del file_handles[path]
                del last_written_timestamps[path]

                # 重命名临时文件（.txt.gz.tmp）为最终的压缩文件（.txt.gz）
                os.rename(path, path.replace('.txt.gz.tmp', '.txt.gz'))

        if file_path in file_handles:
            return file_handles[file_path]
        else:
            # file_handles[file_path] = open(file_path, "a")
            # file_handles[file_path] = gzip.open(file_path, "at")
            file_handles[file_path] = gzip.GzipFile(filename=file_name, mode="wb", fileobj=open(file_path, "wb"))
            return file_handles[file_path]

    try:
        while True:
            data = queue.get()
            if data is None:
                break
            symbol, timestamp, message = data
            dt = datetime.datetime.fromtimestamp(timestamp)
            date = dt.strftime('%Y%m%d')

            # 计算10分钟时间段
            minute_segment = (dt.minute // 10) * 10
            segment_start = dt.replace(minute=minute_segment, second=0, microsecond=0)
            segment_end = segment_start + datetime.timedelta(minutes=10)

            segment_str = segment_start.strftime('%H%M') + '-' + segment_end.strftime('%H%M')

            # 在文件名中包含时间段信息
            file_path = os.path.join(output, f"{symbol}_{date}_{segment_str}.txt.gz.tmp")
            file_name = f"{symbol}_{date}_{segment_str}.txt"
            f = get_file_handle(file_path, file_name, timestamp)
            # f.write(str(int(timestamp * 1000000)))
            # f.write(" ")
            # f.write(message)
            # f.write("\n")

            f.write((str(int(timestamp * 1000000)) + " " + message + "\n").encode("utf-8"))

            last_written_timestamps[file_path] = timestamp
    finally:
        # 确保所有打开的文件在退出循环时被关闭
        for file_handle in file_handles.values():
            file_handle.close()


def shutdown():
    asyncio.create_task(stream.close())


async def main():
    logging.basicConfig(level=logging.DEBUG)
    writer_p = Process(target=writer_proc, args=(queue, sys.argv[3],))
    writer_p.start()
    while not stream.closed:
        await stream.connect()
        await asyncio.sleep(1)
    queue.put(None)
    writer_p.join()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGTERM, shutdown)
    loop.add_signal_handler(signal.SIGINT, shutdown)
    loop.run_until_complete(main())
