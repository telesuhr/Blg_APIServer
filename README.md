# Bloomberg API Bridge

MacbookからWindows PCのBloomberg Terminalにアクセスするためのブリッジシステム

## 概要

このシステムは以下の構成で動作します：
- **Windows側**: Bloomberg TerminalとFastAPIサーバー
- **Macbook側**: Pythonクライアントライブラリ

## セットアップ手順

### 1. Windows側（Bloomberg Terminalがインストールされているマシン）

#### 前提条件
- Bloomberg Terminalがインストールされている
- Python 3.8以上
- Bloomberg Terminalにログイン済み

#### インストール
```cmd
cd bloomberg_bridge\windows_server
pip install -r requirements.txt
```

#### 起動方法
```cmd
start_server.bat
```

または手動で：
```cmd
python bloomberg_api_server.py
```

### 2. Macbook側（クライアント）

#### インストール
```bash
cd bloomberg_bridge/macbook_client
pip install -r requirements.txt
```

#### 接続テスト
```bash
# Windows PCのIPアドレスを指定
python quick_start.py 192.168.1.100
```

## 使用例

### 基本的な使用方法

```python
from bloomberg_client import BloombergClient

# クライアント初期化（Windows PCのIPアドレスを指定）
client = BloombergClient("192.168.1.100", api_key="your-api-key")

# ヒストリカルデータ取得
data = client.get_historical_data(
    securities=["AAPL US Equity", "MSFT US Equity"],
    fields=["PX_LAST", "VOLUME"],
    start_date="2024-01-01",
    end_date="2024-01-31"
)

# CSVエクスポート
client.export_to_csv(data, "bloomberg_data.csv")
```

### 詳細な使用例

`example_usage.py`に10個の実践的な使用例が含まれています。

## 機能

### サーバー機能
- Bloomberg Terminal自動検出
- モックモード（テスト用）
- リクエストキャッシング
- レート制限
- APIキー認証
- エラーハンドリングとログ

### クライアント機能
- 自動リトライ
- クライアントサイドキャッシング
- Pandas DataFrame統合
- 複数のエクスポート形式（CSV, JSON, Excel）
- バッチ処理のサポート

## セキュリティ

### ネットワーク設定
1. Windows Firewall でポート8080を開放
2. 特定のIPアドレスのみ許可することを推奨
3. VPN経由でのアクセスを推奨

### 認証
- APIキーによる認証
- configファイルでAPIキーを管理

## トラブルシューティング

### よくある問題

1. **接続できない**
   - Windows Firewallの設定を確認
   - IPアドレスが正しいか確認
   - ポート8080が開いているか確認

2. **Bloomberg Terminalエラー**
   - Bloomberg Terminalが起動しているか確認
   - BBCommプロセスが実行中か確認
   - Bloomberg Terminalにログインしているか確認

3. **データが取得できない**
   - ティッカーシンボルが正しいか確認
   - フィールド名が正しいか確認
   - Bloombergのライセンスで該当データにアクセス権があるか確認

## モックモード

Bloomberg Terminalがない環境でもテストできるよう、モックモードを用意しています。
サーバーはBloomberg Terminalが起動していない場合、自動的にモックモードで動作します。

## ログ

- Windows側: `bloomberg_api_server.log`
- Macbook側: `bloomberg_client.log`

## ライセンス注意事項

Bloomberg APIの利用規約に従ってください。データの再配布には制限があります。