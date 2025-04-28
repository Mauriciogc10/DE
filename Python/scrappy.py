import scrapy
import random
import time
from scrapy.utils.project import get_project_settings
from scrapy.downloadermiddlewares.retry import get_retry_request
from scrapy.downloadermiddlewares.useragent import UserAgentMiddleware
from scrapy.downloadermiddlewares.httpproxy import HttpProxyMiddleware

class ProductSpider(scrapy.Spider):
    name = "product_spider"
    allowed_domains = ["example.com"]
    start_urls = ["https://www.example.com/products"]

    # Lista de User Agents para rotar
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36",
        "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0"
    ]

    # Lista de proxies (opcional)
    proxies = [
        "http://username:password@proxy1:port",
        "http://username:password@proxy2:port",
        "http://username:password@proxy3:port"
    ]

    def start_requests(self):
        """Genera las solicitudes iniciales con User-Agent y Proxy aleatorio"""
        for url in self.start_urls:
            yield scrapy.Request(
                url, 
                headers={"User-Agent": random.choice(self.user_agents)}, 
                meta={"proxy": random.choice(self.proxies)} 
            )

    def parse(self, response):
        """Parsea la página de productos y extrae información relevante"""
        for product in response.css("div.product-item"):
            yield {
                "name": product.css("h2::text").get(),
                "price": product.css(".price::text").get(),
                "availability": product.css(".stock-status::text").get(),
                "url": response.url
            }

        # Paginación: Encuentra la siguiente página y la sigue
        next_page = response.css("a.next-page::attr(href)").get()
        if next_page:
            yield response.follow(next_page, self.parse)

    def errback(self, failure):
        """Manejo de errores y reintentos"""
        request = failure.request
        if failure.check(scrapy.spidermiddlewares.httperror.HttpError):
            self.logger.error(f"HTTP Error en {request.url}")
        elif failure.check(scrapy.downloadermiddlewares.retry.RetryMiddleware):
            self.logger.error(f"Reintentando {request.url}")
            retry_request = get_retry_request(request, reason=failure.value, spider=self)
            if retry_request:
                return retry_request
        else:
            self.logger.error(f"Error desconocido en {request.url}: {failure}")