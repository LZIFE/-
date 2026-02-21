import requests
import pandas as pd
import json
import time
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import random

class AllStockFinancialCrawler:
    def __init__(self, output_dir="A股财务数据"):
        self.output_dir = output_dir
        self.base_url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
        self.stock_list_url = "http://82.push2.eastmoney.com/api/qt/clist/get"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': 'https://data.eastmoney.com/',
            'Accept': '*/*',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
        self.report_types = {
            '主要财务指标': 'RPT_DMSK_FN_INCOME',
            '资产负债表': 'RPT_DMSK_FN_BALANCE',
            '利润表': 'RPT_DMSK_FN_INCOME',
            '现金流量表': 'RPT_DMSK_FN_CASHFLOW',
        }
        
        self.lock = threading.Lock()
        self.progress = {
            'total': 0,
            'completed': 0,
            'failed': 0,
            'failed_stocks': [],
            'processed_stocks': set()
        }
        
        self.request_interval = 0.5
        self.max_retries = 3
        self.timeout = 30
        
        self._create_directories()
        self._load_progress()
    
    def _create_directories(self):
        for report_type in self.report_types.keys():
            dir_path = os.path.join(self.output_dir, report_type)
            os.makedirs(dir_path, exist_ok=True)
        
        os.makedirs(os.path.join(self.output_dir, "汇总数据"), exist_ok=True)
        os.makedirs(os.path.join(self.output_dir, "日志"), exist_ok=True)
        os.makedirs(os.path.join(self.output_dir, "进度"), exist_ok=True)
    
    def _load_progress(self):
        progress_file = os.path.join(self.output_dir, "进度", "progress.json")
        if os.path.exists(progress_file):
            try:
                with open(progress_file, 'r', encoding='utf-8') as f:
                    saved = json.load(f)
                    self.progress['processed_stocks'] = set(saved.get('processed_stocks', []))
                    self.progress['completed'] = saved.get('completed', 0)
                    self.progress['failed'] = saved.get('failed', 0)
                print(f"已加载进度: 已处理 {len(self.progress['processed_stocks'])} 只股票")
            except Exception as e:
                print(f"加载进度失败: {e}")
    
    def _save_progress(self):
        progress_file = os.path.join(self.output_dir, "进度", "progress.json")
        try:
            with open(progress_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'processed_stocks': list(self.progress['processed_stocks']),
                    'completed': self.progress['completed'],
                    'failed': self.progress['failed'],
                    'last_update': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"保存进度失败: {e}")
    
    def _random_delay(self):
        delay = self.request_interval + random.uniform(0, 0.3)
        time.sleep(delay)
    
    def get_all_stock_list(self):
        print("正在获取A股股票列表...")
        
        all_stocks = []
        page = 1
        page_size = 500
        
        while True:
            params = {
                'pn': page,
                'pz': page_size,
                'po': 1,
                'np': 1,
                'ut': 'bd1d9ddb04089700cf9c27f6f7426281',
                'fltt': 2,
                'invt': 2,
                'fid': 'f3',
                'fs': 'm:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23',
                'fields': 'f12,f14',
                '_': int(time.time() * 1000),
            }
            
            try:
                response = self.session.get(self.stock_list_url, params=params, timeout=self.timeout)
                response.raise_for_status()
                
                text = response.text
                if text.startswith('jQuery'):
                    json_start = text.index('(') + 2
                    json_end = text.rindex(')') - 1
                    json_str = text[json_start:json_end]
                    data = json.loads(json_str)
                else:
                    data = response.json()
                
                if data and 'data' in data and data['data'] and 'diff' in data['data']:
                    stocks = data['data']['diff']
                    for stock in stocks:
                        if isinstance(stock, dict):
                            stock_code = stock.get('f12', '')
                            stock_name = stock.get('f14', '')
                            if stock_code:
                                all_stocks.append({'股票代码': stock_code, '股票名称': stock_name})
                    
                    total_count = data['data'].get('total', 0)
                    print(f"  已获取 {len(all_stocks)}/{total_count} 只股票...")
                    
                    if len(all_stocks) >= total_count:
                        break
                    page += 1
                    self._random_delay()
                else:
                    break
                    
            except Exception as e:
                print(f"获取股票列表失败: {e}")
                break
        
        stock_df = pd.DataFrame(all_stocks)
        
        if '股票代码' not in stock_df.columns or len(stock_df) == 0:
            print("警告: 未能正确获取股票列表")
            return pd.DataFrame(columns=['股票代码', '股票名称'])
        
        stock_list_path = os.path.join(self.output_dir, "汇总数据", "股票列表.xlsx")
        stock_df.to_excel(stock_list_path, index=False)
        print(f"股票列表已保存: {stock_list_path}")
        print(f"共获取 {len(stock_df)} 只A股股票")
        
        return stock_df
    
    def _format_secucode(self, stock_code):
        code = str(stock_code).zfill(6)
        if code.startswith('6'):
            return f"{code}.SH"
        else:
            return f"{code}.SZ"
    
    def _make_request(self, params):
        for attempt in range(self.max_retries):
            try:
                response = self.session.get(self.base_url, params=params, timeout=self.timeout)
                response.raise_for_status()
                data = response.json()
                if data.get('result'):
                    return data
                else:
                    return None
            except requests.exceptions.Timeout:
                if attempt < self.max_retries - 1:
                    time.sleep(2)
            except requests.exceptions.RequestException as e:
                if attempt < self.max_retries - 1:
                    time.sleep(1)
        return None
    
    def get_financial_data(self, secucode, report_name):
        params = {
            'reportName': report_name,
            'columns': 'ALL',
            'filter': f'(SECUCODE="{secucode}")',
            'pageNumber': 1,
            'pageSize': 500,
            'sortColumns': 'REPORT_DATE',
            'sortTypes': '-1',
        }
        
        data = self._make_request(params)
        if data and data.get('result'):
            records = data['result'].get('data', [])
            if records:
                return pd.DataFrame(records)
        return None
    
    def crawl_single_stock(self, stock_code, stock_name):
        secucode = self._format_secucode(stock_code)
        stock_data = {'股票代码': stock_code, '股票名称': stock_name}
        
        self._random_delay()
        
        for report_type, report_name in self.report_types.items():
            try:
                df = self.get_financial_data(secucode, report_name)
                if df is not None and len(df) > 0:
                    stock_data[report_type] = df
            except Exception as e:
                pass
        
        return stock_data
    
    def save_stock_data(self, stock_data):
        stock_code = stock_data['股票代码']
        stock_name = stock_data['股票名称']
        
        safe_name = "".join(c for c in str(stock_name) if c.isalnum() or c in ('-', '_', ' '))
        safe_name = safe_name[:20]
        file_prefix = f"{stock_code}_{safe_name}"
        
        for report_type in self.report_types.keys():
            if report_type in stock_data and stock_data[report_type] is not None:
                df = stock_data[report_type]
                dir_path = os.path.join(self.output_dir, report_type)
                file_path = os.path.join(dir_path, f"{file_prefix}.xlsx")
                
                try:
                    df.to_excel(file_path, index=False, engine='openpyxl')
                except Exception as e:
                    pass
    
    def crawl_all_stocks(self, max_workers=3, stock_limit=None):
        stock_df = self.get_all_stock_list()
        
        if stock_limit:
            stock_df = stock_df.head(stock_limit)
        
        total = len(stock_df)
        self.progress['total'] = total
        
        processed_set = self.progress['processed_stocks']
        remaining_df = stock_df[~stock_df['股票代码'].isin(processed_set)]
        
        if len(remaining_df) == 0:
            print("所有股票已处理完成!")
            return
        
        print(f"\n开始爬取 {len(remaining_df)} 只股票的财务数据 (总共 {total} 只)...")
        print(f"使用 {max_workers} 个线程并发爬取")
        print(f"请求间隔: {self.request_interval}秒")
        print("=" * 60)
        
        start_time = time.time()
        save_interval = 50
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}
            
            for idx, row in remaining_df.iterrows():
                stock_code = row['股票代码']
                stock_name = row['股票名称']
                future = executor.submit(self.crawl_single_stock, stock_code, stock_name)
                futures[future] = (stock_code, stock_name, idx)
            
            for future in as_completed(futures):
                stock_code, stock_name, idx = futures[future]
                
                try:
                    stock_data = future.result()
                    
                    has_data = any(k in stock_data for k in self.report_types.keys())
                    
                    if has_data:
                        self.save_stock_data(stock_data)
                        with self.lock:
                            self.progress['completed'] += 1
                            self.progress['processed_stocks'].add(stock_code)
                    else:
                        with self.lock:
                            self.progress['failed'] += 1
                            self.progress['processed_stocks'].add(stock_code)
                            self.progress['failed_stocks'].append(f"{stock_code}_{stock_name}")
                    
                except Exception as e:
                    with self.lock:
                        self.progress['failed'] += 1
                        self.progress['processed_stocks'].add(stock_code)
                        self.progress['failed_stocks'].append(f"{stock_code}_{stock_name}")
                
                with self.lock:
                    completed = self.progress['completed']
                    failed = self.progress['failed']
                    processed = len(self.progress['processed_stocks'])
                
                if processed % save_interval == 0:
                    self._save_progress()
                
                if processed % 20 == 0:
                    elapsed = time.time() - start_time
                    speed = processed / elapsed if elapsed > 0 else 0
                    remaining = len(remaining_df) - processed
                    eta = remaining / speed / 60 if speed > 0 else 0
                    
                    print(f"进度: {processed}/{len(remaining_df)} ({processed/len(remaining_df)*100:.1f}%) | "
                          f"成功: {completed} | 失败: {failed} | "
                          f"速度: {speed:.1f}只/秒 | 预计剩余: {eta:.1f}分钟")
        
        self._save_progress()
        
        elapsed = time.time() - start_time
        print("=" * 60)
        print(f"爬取完成! 总耗时: {elapsed/60:.1f}分钟")
        print(f"成功: {self.progress['completed']}, 失败: {self.progress['failed']}")
        
        self._save_log()
        self._create_summary()
    
    def _save_log(self):
        log_path = os.path.join(self.output_dir, "日志", f"爬取日志_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
        
        with open(log_path, 'w', encoding='utf-8') as f:
            f.write(f"A股财务数据爬取日志\n")
            f.write(f"爬取时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"{'='*50}\n")
            f.write(f"总股票数: {self.progress['total']}\n")
            f.write(f"成功数量: {self.progress['completed']}\n")
            f.write(f"失败数量: {self.progress['failed']}\n")
            if self.progress['failed_stocks']:
                f.write(f"\n失败股票列表 (共{len(self.progress['failed_stocks'])}只):\n")
                for stock in self.progress['failed_stocks'][:100]:
                    f.write(f"  - {stock}\n")
                if len(self.progress['failed_stocks']) > 100:
                    f.write(f"  ... 还有 {len(self.progress['failed_stocks'])-100} 只\n")
        
        print(f"日志已保存: {log_path}")
    
    def _create_summary(self):
        print("\n正在生成汇总数据...")
        
        for report_type in self.report_types.keys():
            dir_path = os.path.join(self.output_dir, report_type)
            all_data = []
            
            if not os.path.exists(dir_path):
                continue
                
            files = [f for f in os.listdir(dir_path) if f.endswith('.xlsx')]
            
            for i, file in enumerate(files):
                if (i + 1) % 200 == 0:
                    print(f"  {report_type}: 已处理 {i+1}/{len(files)} 个文件")
                
                try:
                    file_path = os.path.join(dir_path, file)
                    df = pd.read_excel(file_path)
                    parts = file.replace('.xlsx', '').split('_', 1)
                    stock_code = parts[0]
                    stock_name = parts[1] if len(parts) > 1 else ''
                    df['股票代码'] = stock_code
                    df['股票名称'] = stock_name
                    all_data.append(df)
                except Exception as e:
                    continue
            
            if all_data:
                combined_df = pd.concat(all_data, ignore_index=True)
                summary_path = os.path.join(self.output_dir, "汇总数据", f"{report_type}_汇总.xlsx")
                combined_df.to_excel(summary_path, index=False, engine='openpyxl')
                print(f"  {report_type} 汇总完成: {len(combined_df)} 条记录")
        
        print("汇总数据生成完成!")


def main():
    print("=" * 60)
    print("A股财务数据爬虫")
    print("=" * 60)
    print("\n安全措施:")
    print("  - 请求间隔: 0.5-0.8秒 (随机)")
    print("  - 并发线程: 3个")
    print("  - 断点续爬: 支持")
    print("  - 进度保存: 每50只股票保存一次")
    print("  - 错误重试: 最多3次")
    print()
    
    crawler = AllStockFinancialCrawler(output_dir="A股财务数据")
    
    crawler.crawl_all_stocks(
        max_workers=3,
        stock_limit=None
    )


if __name__ == "__main__":
    main()
