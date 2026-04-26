"""批量生成研报 - 分批调用API，每批10只，共10批=100只"""
import urllib.request as u, json, os, time

os.environ.pop('http_proxy', None)
os.environ.pop('HTTP_PROXY', None)
u.install_opener(u.build_opener(u.ProxyHandler({})))

STOCKS = ["605577.SH", "002911.SZ", "002756.SZ", "600095.SH", "688197.SH", "000926.SZ", "600548.SH", "301293.SZ", "603335.SH", "002980.SZ", "002376.SZ", "688507.SH", "001359.SZ", "600271.SH", "300446.SZ", "300511.SZ", "301584.SZ", "002363.SZ", "000960.SZ", "688663.SH", "002320.SZ", "603393.SH", "300899.SZ", "603883.SH", "002890.SZ", "601827.SH", "301359.SZ", "601998.SH", "603040.SH", "000566.SZ", "000722.SZ", "601019.SH", "002091.SZ", "301197.SZ", "600136.SH", "603610.SH", "300175.SZ", "605208.SH", "605169.SH", "301171.SZ", "600048.SH", "002761.SZ", "603757.SH", "600975.SH", "000651.SZ", "688091.SH", "600629.SH", "301419.SZ", "000783.SZ", "300683.SZ", "601187.SH", "605196.SH", "688659.SH", "300725.SZ", "003015.SZ", "300864.SZ", "300395.SZ", "603106.SH", "600435.SH", "002628.SZ", "002629.SZ", "300073.SZ", "601858.SH", "600515.SH", "001368.SZ", "000568.SZ", "300003.SZ", "002290.SZ", "000605.SZ", "688798.SH", "603360.SH", "300703.SZ", "301323.SZ", "300688.SZ", "000998.SZ", "000421.SZ", "001391.SZ", "600261.SH", "300809.SZ", "300288.SZ", "002088.SZ", "002069.SZ", "301208.SZ", "600633.SH", "603608.SH", "002758.SZ", "002203.SZ", "301501.SZ", "300912.SZ", "002872.SZ", "688111.SH", "002946.SZ", "605339.SH", "688616.SH", "603248.SH", "300626.SZ", "603444.SH", "301187.SZ", "002180.SZ", "300603.SZ"]

BATCH_SIZE = 10
TRADE_DATE = '2026-04-03'
TOKEN = 'kestra-internal-20260327'
BASE_URL = 'http://127.0.0.1:8010'

total_ok = 0
total_fail = 0
start_all = time.time()

batches = [STOCKS[i:i+BATCH_SIZE] for i in range(0, len(STOCKS), BATCH_SIZE)]
print(f"Total stocks: {len(STOCKS)}, batches: {len(batches)}")

for i, batch in enumerate(batches):
    print(f"\n--- Batch {i+1}/{len(batches)}: {batch} ---")
    payload = {
        'stock_codes': batch,
        'trade_date': TRADE_DATE,
        'force': True,
        'skip_pool_check': True,
        'cleanup_incomplete_before_batch': False,
    }
    req = u.Request(
        f'{BASE_URL}/api/v1/internal/reports/generate-batch',
        data=json.dumps(payload).encode(),
        headers={'Content-Type': 'application/json', 'X-Internal-Token': TOKEN},
        method='POST'
    )
    t = time.time()
    try:
        resp = json.loads(u.urlopen(req, timeout=600).read())
        d = resp.get('data', {})
        ok = d.get('succeeded', 0)
        total = d.get('total', 0)
        total_ok += ok
        total_fail += (total - ok)
        print(f"  Result: {ok}/{total} ok in {time.time()-t:.0f}s")
        for item in d.get('details', []):
            status = item.get('status', '?')
            code = item.get('stock_code', '?')
            err = item.get('error_code', '-')
            qual = item.get('quality_flag', '?')
            if status != 'ok':
                print(f"  FAIL {code}: {status} {err}")
    except Exception as e:
        total_fail += len(batch)
        print(f"  Batch ERROR: {e}")

elapsed = time.time() - start_all
print(f"\n=== DONE: {total_ok} ok, {total_fail} fail, {elapsed:.0f}s total ===")
