import requests

def fetch_html_with_proxy(url, proxy):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
    }

    # 配置代理设置
    proxies = {
        'http': proxy,
        'https': proxy
    }

    try:
        response = requests.get(
            url,
            headers=headers,
            proxies=proxies,
            timeout=15,
            verify=True
        )

        # 检查请求是否成功
        response.raise_for_status()

        # 检测实际内容类型
        content_type = response.headers.get('Content-Type', '')
        if 'text/html' not in content_type:
            print(f"警告：返回内容可能不是HTML (Content-Type: {content_type})")

        # 返回HTML内容
        return response.text

    except requests.exceptions.SSLError as e:
        print("SSL错误:", e)
        print("尝试关闭SSL验证...")
        try:
            response = requests.get(
                url,
                headers=headers,
                proxies=proxies,
                timeout=15,
                verify=False
            )
            response.raise_for_status()
            return response.text
        except Exception as e2:
            print("第二次尝试失败:", e2)
            return None

    except requests.exceptions.ProxyError as e:
        print("代理连接失败:", e)
        print("请检查：1. 代理地址是否正确 2. 代理服务是否运行 3. 防火墙设置")
        return None

    except requests.exceptions.RequestException as e:
        print(f"请求出错: {e}")
        return None


if __name__ == "__main__":

    url = "https://www.reuters.com/business/energy/russias-july-seaborne-oil-product-exports-down-66-mm-reuters-calculations-show-2025-08-13/"

    proxy_address = "http://127.0.0.1:7890"
    # proxy_address = "socks5://127.0.0.1:7890"  # SOCKS5代理

    print(f"正在通过代理 {proxy_address} 获取内容...")
    html_content = fetch_html_with_proxy(url, proxy_address)

    if html_content:
        # 将结果保存到文件
        filename = "reuters_article.html"
        with open(filename, "w", encoding="utf-8") as f:
            f.write(html_content)
        print(f"成功获取内容！已保存到 {filename}")

        # 打印前500个字符预览
        print("\nHTML预览:")
        print(html_content[:500] + "...")
    else:
        print("未能获取HTML内容")