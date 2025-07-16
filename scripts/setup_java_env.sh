#!/bin/bash
# 設置 Java 11 環境變量腳本

# 檢查 Java 11 是否已安裝
JAVA_11_PATH="/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home"

if [ -d "$JAVA_11_PATH" ]; then
    echo "✅ Java 11 已安裝在: $JAVA_11_PATH"
    export JAVA_HOME="$JAVA_11_PATH"
    export PATH="$JAVA_HOME/bin:$PATH"
    
    echo "✅ 設置 JAVA_HOME: $JAVA_HOME"
    echo "✅ 當前 Java 版本:"
    java -version
else
    echo "❌ Java 11 未安裝，正在安裝..."
    brew install openjdk@11
    
    if [ $? -eq 0 ]; then
        echo "✅ Java 11 安裝成功"
        export JAVA_HOME="$JAVA_11_PATH"
        export PATH="$JAVA_HOME/bin:$PATH"
        
        echo "✅ 設置 JAVA_HOME: $JAVA_HOME"
        echo "✅ 當前 Java 版本:"
        java -version
    else
        echo "❌ Java 11 安裝失敗"
        exit 1
    fi
fi

# 將環境變量寫入 ~/.zshrc (如果使用 zsh)
if [ -n "$ZSH_VERSION" ]; then
    echo "📝 添加 Java 11 環境變量到 ~/.zshrc"
    echo "" >> ~/.zshrc
    echo "# Java 11 for Spark101" >> ~/.zshrc
    echo "export JAVA_HOME=\"$JAVA_11_PATH\"" >> ~/.zshrc
    echo "export PATH=\"\$JAVA_HOME/bin:\$PATH\"" >> ~/.zshrc
    
    echo "✅ 環境變量已添加到 ~/.zshrc"
    echo "💡 請執行 'source ~/.zshrc' 或重新啟動終端來應用變更"
fi

# 測試 Spark 環境
echo "🧪 測試 Spark 環境..."
poetry run python test_spark_setup.py