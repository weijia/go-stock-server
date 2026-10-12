#!/usr/bin/env bash
# 本地沙箱启用代理（GitHub Actions 禁用）—— 从 GRADLE_PROXY_HOST/PORT 注入
set -e
DIR="$(cd "$(dirname "$0")" && pwd)"
F="$DIR/gradle.properties"
[ -n "$GRADLE_PROXY_HOST" ] || { echo "需设置 GRADLE_PROXY_HOST"; exit 1; }
PORT=${GRADLE_PROXY_PORT:-8080}
# 去重（先删掉旧的代理行）
sed -i '/^systemProp\.(http|https|http\.nonProxyHosts|https\.nonProxyHosts)/d' "$F"
cat >> "$F" <<EOF
systemProp.http.proxyHost=$GRADLE_PROXY_HOST
systemProp.http.proxyPort=$PORT
systemProp.https.proxyHost=$GRADLE_PROXY_HOST
systemProp.https.proxyPort=$PORT
systemProp.http.nonProxyHosts=localhost|127.0.0.1|10.*|.local
systemProp.https.nonProxyHosts=localhost|127.0.0.1|10.*|.local
EOF
chmod +x android-app/setup_proxy.sh
