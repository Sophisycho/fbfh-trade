# fbfh-trade

> 批次查詢「出進口廠商登記系統」資料、篩選評級、取得公司詳細資訊，並一鍵匯出 Excel。
> 適合名單蒐集、資料分析與後續聯繫整備。


---

## 目錄

* [專案功能](#專案功能)
* [資料流程](#資料流程)
* [快速開始](#快速開始)
* [執行方式](#執行方式)
* [輸出檔案與格式](#輸出檔案與格式)
* [錯誤處理與續跑機制](#錯誤處理與續跑機制)

---

## 專案功能

1. **批次掃描統一編號**

   * 自動產生合法 8 碼統編並逐一查詢指定年度的**進出口實績評級**。
   * 將所有「正常回應」寫入 `ok.json`，將**評級屬 A\~K** 的命中結果寫入 `hits.json`。

2. **公司詳細資料**

   * 針對 `hits.json` 的公司逐一取得**完整登記資料**（中英文名稱、地址、代表人、網站/Email、進出口資格與項目…），彙整成 `company_details.json`。

3. **一鍵匯出 Excel**

   * 以固定欄位順序輸出 `company_details.xlsx`（每家公司/年度為一列，便於後續分析或聯繫）。

4. **穩健性**

   * 具 **429 頻控退避重試**、**驗證碼（verifySHidden）自動刷新**、**致命錯誤即時停機並保留進度**、**續跑** 等機制。

> 📌 **最新變更（摘要）**
>
> * 新增：公司詳細資料抓取與 `company_details.json` 生成流程。
> * 新增：`company_details.xlsx` 匯出工具。
> * 強化：429/非 JSON/驗證失效等**錯誤自動處理**與**續跑**。

---

## 資料流程

```
runner.py  →  取得評級資料
   ├─ ok.json   （所有正常回應）
   ├─ hits.json （評級 A~K 的命中）
   └─ state.json（續跑進度）

main.py    →  針對 hits.json 取公司詳細資訊 → company_details.json → company_details.xlsx
```

> 所有檔案預設**讀寫於執行檔所在目錄**。打包後（.exe）亦相同：請把 `.exe` 與欲讀寫的 `.json` 放在**同一目錄**。

---

## 快速開始

### 1) 取得程式碼

```bash
git clone git@github.com:Sophisycho/fbfh-trade.git
cd fbfh-trade
```

### 2) 建立虛擬環境並安裝相依

（Windows PowerShell）

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

若沒有 `requirements.txt`，可先安裝最小集：

```powershell
python -m pip install requests openpyxl beautifulsoup4 brotli
```

> `beautifulsoup4`、`brotli` 為建議項；缺少時程式亦能用後備方式運作，但建議安裝以提高成功率與相容性。

---

## 執行方式

### 1) 批次掃描（評級）

```bash
python runner.py --year 113 --sleep 0.1
```

**常用參數**

* `--year <int>`：查詢評等年度（民國年，例：113 = 2024 年）。
* `--sleep <float>`：每筆查詢之間的最小延遲（秒），建議保留以降低觸發頻控風險。

執行中會看到：

* `OK <統編> ... name_zh=<公司名稱>`：表示**正常回應**，寫入 `ok.json`。
* `HIT <統編> ... import=<代碼> export=<代碼>`：為**評級 A\~K 命中**，寫入 `hits.json`。

### 2) 生成詳細資料並匯出 Excel

```bash
python main.py
```

流程：

1. 讀取 `hits.json`（若為空則不進行）。
2. 針對每家公司抓取詳細資料 → 產生 `company_details.json`。
3. 依固定欄位順序輸出 `company_details.xlsx`。

---

## 輸出檔案與格式

* **`ok.json`**

  * 以 `統編` 為鍵，內含各年度的基本資料（例如公司名稱）。
* **`hits.json`**

  * 結構類似 `ok.json`，但僅收錄評級 **A\~K** 的公司（命中名單）。
* **`state.json`**

  * 保存目前掃描進度（下次可續跑）。
* **`company_details.json`**

  * `{"<統編>": {"<年度>": {...詳細欄位...}}}`
* **`company_details.xlsx`**（主要欄位示例）

  * `business_account_no, rating_year, import_total_code, export_total_code, company_name_zh, company_name_en, representative, business_address_zh, business_address_en, date_of_last_change, original_registration_date, former_company_name_zh, former_company_name_en, website, email, import_eligibility, export_eligibility, import_items[], export_items[] ...`

> 欄位可能因官方網站調整而增減，程式會盡力維持兼容。

---

## 錯誤處理與續跑機制

* **429 Too Many Requests**：採用**伺服器提示的 Retry-After 或指數退避**重試，同一統編不丟失。預設不中止（可依程式設定限制最大重試）。
* **驗證碼（verifySHidden）失效**：自動呼叫內建流程刷新 token，再重試一次；仍失敗則嘗試替代提交方式。
* **非 JSON/非 200**：視為致命錯誤，**即時停機**並保存 `state.json`/`hits.json`/`ok.json`，避免污染。
* **中斷續跑**：`Ctrl + C` 時保存進度；下次執行自動從 `state.json` 所記錄位置續跑。

---