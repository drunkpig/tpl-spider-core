import os, re
from logging.config import fileConfig
import shutil
from utils import get_date, get_domain, get_abs_url, format_url, \
    is_same_web_site_link, is_img_ext, is_under_same_link_folder, is_page_url, is_inline_resource, \
    get_file_name_from_url, base64_encode_resource
from datetime import datetime
import time
from bs4 import BeautifulSoup
import config
from queue import Queue, Empty
import threading
import aiohttp
import asyncio
import aiofiles
from config import logger


class TemplateCrawler(object):
    logger = logger
    CMD_DOWNLOAD = 'download'
    CMD_QUIT = 'quit'
    FILE_TYPE_BIN = 'bin'
    FILE_TYPE_TEXT = 'text'

    def __init__(self, url_list, save_base_dir, header, encoding=None, grab_out_site_link=False, to_single_page=False,
                 full_site=False, ref_model=False, framework=None):
        """

        :param url_list:
        :param save_base_dir:
        :param header:
        :param encoding:
        :param grab_out_site_link:
        :param to_single_page:
        :param full_site:
        :param ref_model:
        """
        self.parent_save_dir = save_base_dir
        self.date_str = get_date()
        self.zip_save_base_abs_dir = f"{self.parent_save_dir}/{config.template_archive_dir}/{self.date_str}/"  # zip /xx/xx/archive/2019-00-01/
        self.download_temp_abs_dir = f"{self.parent_save_dir}/{config.template_temp_dir}/{self.date_str}/"
        self.domain = get_domain(url_list[0])
        self.tpl_dl_dir, self.js_dir, self.img_dir, self.css_dir, self.other_dir = self.__prepare_dirs()
        self.dl_urls = {}  # 去重使用,存储 url=>磁盘绝对路径
        self.error_grab_resource = {}  # 记录 http url => disk url ，最后生成一个报告打包
        self.header = header
        self.charset = encoding
        self.is_grab_outer_link = grab_out_site_link
        self.is_ref_model = ref_model
        if self.is_ref_model:
            self.is_grab_outer_link = False  # 盗链模式下，一定不抓外部的资源,内部资源也会被改写绝对路径
        self.is_to_single_page = to_single_page  # 是否把图片，css, js等压缩到一个页面里
        self.single_page = []  # report生成时使用
        self.is_full_site = full_site  # 是否是整站
        self.html_link_queue = Queue()  # html 页面的队列
        for u in url_list:
            self.html_link_queue.put(u)
        self.downloaded_html_url = []  # 已经下载过的html，保存(disk_path, file_name, url, ), 最后用这个重新修正链接
        self.download_queue = Queue()  # 数据格式json  {'cmd':quit/download, "url":'http://baidu.com', "save_path":'/full/path/file.ext', 'type':'bin/text'}
        self.download_finished = False  # url消耗完毕不代表网络请求都返回了
        self.task_finished = False  # 全部网络都返回， eventloop结束
        self.zip_result_file = None
        self.file_name_dup_checker = {}  # file_name => url 。用于检查生成的文件名字是否有重复的，如果重复了就要重新生成了
        self.framework_support = framework
        self.thread = threading.Thread(target=self.__download_thread)
        self.thread.start()

    def __get_relative_report_file_path(self, path):
        path = path.replace("//", "/")
        return path[len(self.__get_tpl_full_path()):]

    async def __make_report(self):
        """
        1，标题
        2，源url
        3，error url
        4，ok url
        TODO js里的替换,保存原始文件和替换后文件
        :return:
        """
        report_file = f"{self.__get_tpl_full_path()}/_report.html"
        async with aiofiles.open(report_file, 'w', encoding='utf-8') as f:
            await f.writelines("""
                <style>
                a.key{
                color: green;
                }
                a.value{
                
                }
                a.err{
                    color: red;
                }
                </style>
                <center><h1>TEMPLATE REPORT</h1></center><br>\n
                
                <h2 style='color: red;'>Error report</h2><br>\n
            """)
            if len(self.error_grab_resource.keys()) > 0:
                for url, path in self.error_grab_resource.items():
                    await f.writelines(
                        f"""
                        <a class='err' href='{url}' target="_blank"> {url} </a>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; => &nbsp;&nbsp;&nbsp;&nbsp;&nbsp; {path} <br>\n
                        """
                    )

                await f.writelines("""
                <b>To fix this error: download the url content and put them in the followed directory.</b><br>\n
            """)
            else:
                await f.writelines("every thing is ok!")
            ## =========================================== Email页面
            if self.is_to_single_page:
                await f.writelines("""
                                                <hr /><br>
                                                <h2>single page template</h2><br>\n
                                            """)
                for u in self.single_page:
                    await f.writelines(f"<a target='_blank' class='key' href='{u}'>{u}</a> <br>\n")
            # #===========================================用户输入的原始页面+派生的html
            await f.writelines("""
            <hr /><br>
            <h2>Template source url</h2><br>\n
        """)
            for disk_file, file, url in self.downloaded_html_url:
                await f.writelines(
                    f"<a target='_blank' class='key' href='{url}'>{url}</a> &nbsp;&nbsp;&nbsp;&nbsp;&nbsp; =>  <a class='value' target='_blank' href='./{file}'>{file}</a> <br>\n")
            # #######################===========全部下载资源
            await f.writelines(f"""
            <hr />
            
            <h2>Spider report ({len(self.dl_urls.keys())} files)</h2><br>\n
            """)

            for url, path in self.dl_urls.items():
                await f.writelines(
                    f"<a class='key' href='{url}'>{url}</a> &nbsp;&nbsp;&nbsp;&nbsp;&nbsp; =>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; <a class='value' target='_blank' href='{self.__get_relative_report_file_path(path)}'>{self.__get_relative_report_file_path(path)}</a> <br>\n")

            await f.writelines("""
            <br><br>
            <hr/>
            <center><a href='http://template-spider.com'>web template spider</a>&nbsp;|&nbsp;<a href=''>report bug</a></center>
        """)

    def __is_dup(self, url, save_path):
        """
        放置重复抓取
        :param url:
        :param save_path:
        :return:
        """
        save_path = save_path.replace("//", '/')
        if url in self.dl_urls.keys():
            save_path2 = self.dl_urls.get(url)
            if save_path == save_path2:
                self.logger.debug("cached %s", url)
                return True

        return False

    def __set_dup_url(self, url, file_save_path):
        url = format_url(url)
        if file_save_path:
            file_save_path = file_save_path.replace("//", '/')
        self.dl_urls[url] = file_save_path

    def __get_tpl_full_path(self):
        p = f"{self.download_temp_abs_dir}/{self.tpl_dl_dir}/"
        p = p.replace("//", '/')
        return p

    def __get_tpl_dir(self):
        return self.tpl_dl_dir

    def __get_save_base_dir(self):
        return self.zip_save_base_abs_dir

    def __get_zip_relative_path(self, full_path):
        """
        /home/cxu/zip/2019-01-02/xxxx.zip   ->  2019-01-02/xxxx.zip
        :param full_path:
        :return:
        """
        zip_file = "%s.zip" % full_path[len(f'{self.parent_save_dir}') + 1:]
        return zip_file

    def __get_zip_full_path(self):
        zip_base_dir = self.zip_save_base_abs_dir
        if not os.path.exists(zip_base_dir):
            os.makedirs(zip_base_dir)
        zip_file_path = f"{zip_base_dir}/{self.tpl_dl_dir}"

        return zip_file_path

    def __get_img_full_path(self):
        return f"{self.__get_tpl_full_path()}/{self.img_dir}"

    def __get_css_full_path(self):
        return f"{self.__get_tpl_full_path()}/{self.css_dir}"

    def __get_js_full_path(self):
        return f"{self.__get_tpl_full_path()}/{self.js_dir}"

    @staticmethod
    async def __async_save_text_file(content, file_abs_path, encoding='utf-8'):
        async with aiofiles.open(file_abs_path, "w", encoding=encoding) as f:
            await f.writelines(content)

    def __prepare_dirs(self):
        """
        根据域名+ts命名这个目录
        :return:
        """
        template_dir = f"{self.domain}_{datetime.now().timestamp()}"  # 日期目录/下的  域名_时间戳
        tpl_full_dir = f"{self.download_temp_abs_dir}/{template_dir}"
        dirs = [tpl_full_dir, f"{tpl_full_dir}/js", f"{tpl_full_dir}/img", f"{tpl_full_dir}/css",
                f"{tpl_full_dir}/video", ]
        for d in dirs:
            if not os.path.exists(d):
                os.makedirs(d)

        return template_dir, "js", "img", "css", "other"

    def __get_file_name(self, url, i):
        return f"index_{i}.html"

    def __log_error_resource(self, url, path):
        self.error_grab_resource[url] = path

    def __dl_js(self, soup, url):
        """
        下载js，替换html里js文件的地址
        :param soup:
        :param url:
        :return:
        """
        scripts_urls = soup.find_all("script")
        for scripts in scripts_urls:
            raw_link = scripts.get("src")
            if raw_link is None:
                continue
            abs_link = get_abs_url(url, raw_link)

            if self.is_ref_model:
                scripts['src'] = abs_link
            elif is_same_web_site_link(url, abs_link) or self.is_grab_outer_link:
                """
                如果是外链引入的js就不管了,除非打开了开关
                """
                file_name = get_file_name_from_url(abs_link, self.file_name_dup_checker, "js")
                file_save_path = f"{self.__get_js_full_path()}/{file_name}"
                replace_url = f"{self.js_dir}/{file_name}"
                scripts['src'] = replace_url
                self.__url_enqueue(abs_link, file_save_path, self.FILE_TYPE_TEXT)
                # 将跨域锁定和来源校验关闭
                if scripts.get("crossorigin") is not None:
                    del scripts['crossorigin']
                if scripts.get('integrity') is not None:
                    del scripts['integrity']
            else:
                scripts['src'] = abs_link

    def __dl_img(self, soup, url):
        """
        下载图片，并替换html里图片的地址
        :param soup:
        :param url:
        :return:
        """
        images = soup.find_all("img")
        for img in images:
            raw_link = img.get("src")
            if raw_link is None or is_inline_resource(raw_link):  # 跳过base64内嵌图片 <img src='data:image...'/>
                continue
            abs_link = get_abs_url(url, raw_link)

            if self.is_ref_model:
                img['src'] = abs_link
            elif is_same_web_site_link(url, abs_link) or self.is_grab_outer_link:
                file_name = get_file_name_from_url(abs_link, self.file_name_dup_checker, "png")
                file_save_path = f"{self.__get_img_full_path()}/{file_name}"
                replace_url = f"{self.img_dir}/{file_name}"
                img['src'] = replace_url

                self.__url_enqueue(abs_link, file_save_path, self.FILE_TYPE_BIN)
            else:  # 修正为绝对链接
                img['src'] = abs_link

            if img.get("crossorigin ") is not None:
                del img['crossorigin ']
            if img.get("integrity") is not None:
                del img['integrity']

    @staticmethod
    def __get_style_url_link(url_src):
        """
        url('xxxx')
        url("xxxx")
        url(xxxx)
        http://xxxx
        :param url_src:
        :return:  xxxx
        """
        url_src = url_src.strip()
        if '"' in url_src or "'" in url_src:
            return url_src[5: -2].strip()
        elif url_src.startswith(('http', "https")):
            return url_src
        else:
            return url_src[4: -1].strip()

    def __dl_in_element_style_img(self, soup, url):
        """
        获取到html页面内嵌样式的图片资源
        <xx style='background: url(xxxxx.jpg)'>
        :param soup:
        :param url:
        :return:
        """
        inner_style_node = soup.find_all(style=re.compile("url(.*?)"))  # TODO url/URL 大小写
        for style in inner_style_node:
            resource_url = re.findall('url\(.*?\)', style.get("style"))[0]  # TODO 遍历而非取第一个，匹配到全部
            resource_url = self.__get_style_url_link(resource_url)
            if is_inline_resource(resource_url):  # 内嵌base64图片
                continue
            abs_link = get_abs_url(url, resource_url)
            if self.is_ref_model:
                style['style'] = style['style'].replace(resource_url, abs_link)
            elif is_same_web_site_link(url, abs_link) or self.is_grab_outer_link:
                file_name = get_file_name_from_url(abs_link, self.file_name_dup_checker, 'png')
                file_save_path = f"{self.__get_img_full_path()}/{file_name}"
                replace_url = f"{self.img_dir}/{file_name}"
                replace_url = replace_url
                style['style'] = style['style'].replace(resource_url, replace_url)
                self.__url_enqueue(abs_link, file_save_path, self.FILE_TYPE_BIN)
            else:
                style['style'] = style['style'].replace(resource_url, abs_link)

    async def __process_in_html_css_resource(self, soup, url):  # TODO  @import url("../fonts/font-awesome/css/font-awesome.css"); http://www.qdujava.com/user/css/style.css
        style_css = soup.find_all("style")
        if style_css is None:
            return
        for style in style_css:
            css_content = style.text
            if css_content is None:
                continue
            all_urls = re.findall('url\(.*?\)', css_content)
            if all_urls is not None:
                for raw_u in all_urls:
                    u = self.__get_style_url_link(raw_u)  # > url('xxx') or url(xxx)
                    if is_inline_resource(u):  # 内嵌base64图片
                        continue
                    abs_link = get_abs_url(url, u)
                    if self.is_ref_model:
                        css_content = css_content.replace(raw_u, f'url({abs_link})')
                    elif is_same_web_site_link(url, abs_link) or self.is_grab_outer_link:
                        file_name = get_file_name_from_url(abs_link, self.file_name_dup_checker)
                        if is_img_ext(file_name):
                            file_save_path = f"{self.__get_img_full_path()}/{file_name}"
                            replace_url = f"{self.img_dir}/{file_name}"
                        else:
                            file_save_path = f"{self.__get_css_full_path()}/{file_name}"
                            replace_url = f"{self.css_dir}/{file_name}"
                        css_content = css_content.replace(raw_u, f'url({replace_url})')
                        self.__url_enqueue(abs_link, file_save_path, self.FILE_TYPE_BIN)
                    else:  # 替换，特别是以//开头那种url
                        css_content = css_content.replace(raw_u, f'url({abs_link})')
                style.string = css_content

    async def __dl_link(self, soup, url):
        """
        下载<link>标签里的资源，并替换html里的地址
        # TODO 处理 css 里的 @import
        :param soup:
        :param url:
        :return:
        """
        css_src = soup.find_all("link")
        if css_src is None:
            return
        for css in css_src:
            raw_link = css.get("href")
            if raw_link is None:
                continue
            abs_link = get_abs_url(url, raw_link)

            if self.is_ref_model:
                css['href'] = abs_link
            elif is_same_web_site_link(url, abs_link) or self.is_grab_outer_link:  # 控制是否抓外链资源
                file_name = get_file_name_from_url(abs_link, self.file_name_dup_checker, 'css')

                if is_img_ext(file_name):
                    file_save_path = f"{self.__get_img_full_path()}/{file_name}"
                    replace_url = f"{self.img_dir}/{file_name}"
                    self.__url_enqueue(abs_link, file_save_path, self.FILE_TYPE_BIN)
                else:
                    file_save_path = f"{self.__get_css_full_path()}/{file_name}"
                    replace_url = f"{self.css_dir}/{file_name}"
                    if not self.__is_dup(abs_link, file_save_path):
                        resp_text, _ = await self.__async_get_request_text(abs_link, force_as_text=True)
                        if resp_text is not None:
                            text_content = resp_text
                            text_content = await  self.__replace_and_grab_css_url(abs_link, text_content)
                            self.__set_dup_url(abs_link, file_save_path)
                            await self.__async_save_text_file(text_content, file_save_path)  # 存储css文件
                        else:
                            self.__log_error_resource(abs_link, file_save_path)

                css['href'] = replace_url
            else:  # 修正link url为绝对链接地址
                css['href'] = abs_link

            # 将跨域锁定和来源校验关闭
            if css.get("crossorigin") is not None:
                del css['crossorigin']
            if css.get('integrity') is not None:
                del css['integrity']

    async def __replace_and_grab_css_url(self, url, text):
        """
        @import url(font-awesome.min.css);
        @import "https://fonts.googleapis.com/css?family=Montserrat:700|Open+Sans:300,400|Pacifico";
        :param url:
        :param text:
        :return:
        """
        urls = re.findall("url\(.*?\)", text)  # TODO 区分大小写
        for u in urls:
            relative_u = self.__get_style_url_link(u)
            if is_inline_resource(relative_u):  # 内嵌base64图片
                continue

            abs_link = get_abs_url(url, relative_u)
            if relative_u.endswith("css"):  # 这一步把带有 @import url(xx)的忽略掉，防止和下一步重合
                self.logger.warning("skip css file, grab in the next step: %s", abs_link)
                continue

            if self.is_grab_outer_link:  # 控制是否抓外链资源,只要抓外部资源，那么css里的全部资源都要无条件抓进来而不管是不是一个同站点的
                file_name = get_file_name_from_url(abs_link, self.file_name_dup_checker, 'css')
                is_img = is_img_ext(file_name)
                if is_img:
                    file_save_path = f"{self.__get_img_full_path()}/{file_name}"
                    replace_url = f"../{self.img_dir}/{file_name}"
                    self.__url_enqueue(abs_link, file_save_path, self.FILE_TYPE_BIN)
                    text = text.replace(relative_u, replace_url)
                else:
                    file_save_path = f"{self.__get_css_full_path()}/{file_name}"
                    replace_url = f"{file_name}"  # 由于是相对于css文件的引入,因此是平级关系, 如果是图片就需要从../img目录下
                    self.__url_enqueue(abs_link, file_save_path, self.FILE_TYPE_BIN)
                    text = text.replace(relative_u, replace_url)

        imported_css = re.findall('@import\s+["\']+(.*?)["\']', text)  # css里 @import的情况
        imported_css2 = re.findall('@import\s+url\(.*?\)', text)  # > ['@import url(font-awesome.min.css)']
        if imported_css2 is not None:
            for x in imported_css2:
                x = self.__get_style_url_link(x.split()[1])
                imported_css.append(x)

        if imported_css:
            for u in imported_css:
                if u.startswith(("http", "https")):
                    abs_link = u
                else:
                    abs_link = get_abs_url(url, u)

                file_name = get_file_name_from_url(abs_link, self.file_name_dup_checker, 'css')
                file_save_path = f"{self.__get_css_full_path()}/{file_name}"
                if not self.__is_dup(abs_link, file_save_path):
                    resp_text, _ = await self.__async_get_request_text(abs_link)

                    if resp_text is not None:
                        text_content = resp_text
                        text_content = await self.__replace_and_grab_css_url(abs_link, text_content)
                        self.__set_dup_url(abs_link, file_save_path)
                        # 在存储之前要把返回的css里的@import后面内容替换掉
                        text_content = text_content.replace(u, file_name)
                        await self.__async_save_text_file(text_content, file_save_path)  # 存储css文件
                    else:
                        self.__log_error_resource(url, file_save_path)  # 下载失败的

        return text

    async def __rend_template(self, soup, url):
        """
        把从url抓到的html原始页面进行链接加工，图片，css,js下载
        :param url:
        :param html:
        :return:
        """

        self.__dl_in_element_style_img(soup, url)
        self.__dl_img(soup, url)
        await self.__dl_link(soup, url)
        self.__dl_js(soup, url)
        await self.__process_in_html_css_resource(soup, url)
        return soup

    def __url_enqueue(self, url='', file_save_path='', file_type='text'):
        """

        :param cmd: quit/download
        :param url: url or None
        :param file_save_path: full path or None
        :param file_type: bin/text
        :return:
        """
        file_save_path = file_save_path.replace("//", '/')
        if not self.__is_dup(url, file_save_path):
            self.__set_dup_url(url, file_save_path)
            self.download_queue.put({
                'cmd': self.CMD_DOWNLOAD,
                'url': url,
                'file_save_path': file_save_path,
                'file_type': file_type,
            })

    def __quit_cmd_enqueue(self):
        self.download_queue.put({
            'cmd': self.CMD_QUIT,
            'url': '',
            'file_save_path': '',
            'file_type': '',
        })

    def __wait_unitl_task_finished(self):
        """
        TODO 上报状态到redis里
        :return:
        """
        while True:
            if not self.download_finished:
                self.logger.info("task not finish, wait. Left %s URL.", self.download_queue.qsize())
                time.sleep(config.wait_download_finish_sleep)
                if not self.task_finished:
                    self.logger.info("url process finished, waiting request finish.")
                    time.sleep(2)
            elif self.task_finished:
                break

    async def __async_get_request_text(self, url, force_as_text=False):
        max_retry = config.max_retry
        time_out = config.http_timeout
        for i in range(1, max_retry + 1):
            to = time_out * i
            try:
                logger.info("async craw[%s] %s", i, url)
                async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(verify_ssl=False)) as session:
                    async with session.get(url, timeout=to, headers=self.header) as resp:
                        if resp.status != 200:
                            continue
                        elif resp.status == 404:
                            break
                        elif not resp.content_type.startswith("text") and force_as_text is False:  # 如果是二进制的，防止错误
                            break
                        txt = await resp.text()
                        encoding = resp.charset
                return txt, encoding
            except Exception as e:
                if i < max_retry:
                    continue
                else:
                    logger.error(e)
        return None, None

    def __pre_process_page(self, soup, url):
        delete_node_attr = [{"link": ("rel", "alternate")}, {"link": ("rel", "dns-prefetch")},
                            {"link": ("rel", "manifest")}]
        for rule in delete_node_attr:
            for k, v in rule.items():
                nodes = soup.find_all(k)
                if nodes:
                    for n in nodes:
                        attr_name, attr_val = v
                        attr = n.get(attr_name)
                        if attr and attr_val in attr:
                            n.decompose()
        return soup

    async def template_crawl(self):
        """
        把url_list里的网页全部抓出来当做模版，
        存储到save_path/${date}/目录下

        :param url_list:
        :param save_path:
        :return:
        """
        i = 0
        url = self.html_link_queue.get(timeout=10)
        html_dedup_list = [url]
        while url is not None:
            tpl_file_name = self.__get_file_name(url, i)
            save_file_path = f"{self.__get_tpl_full_path()}/{tpl_file_name}"

            if not self.__is_dup(url, tpl_file_name):
                resp_text, encoding = await self.__async_get_request_text(url)
                html = resp_text

                if html is not None:
                    if self.charset is None:
                        self.charset = encoding
                        if self.charset is None:
                            self.charset = 'utf-8'

                    soup = BeautifulSoup(html, "lxml")
                    soup = self.__pre_process_page(soup, url)
                    soup = await self.__rend_template(soup, url)  # 这里是个核心函数，下载页面里的image, js, css等资源

                    new_url = self.__get_same_site_link(soup, url)  # 获取全部同网站下的链接页面，当然会检查一下是否要全栈抓取
                    for u in new_url:  # 新产生的url进入队列，用于全站抓取时候
                        if is_same_web_site_link(u, url) and is_under_same_link_folder(u,
                                                                                       url) and u not in html_dedup_list:  # 同站，同目录，非重复
                            self.html_link_queue.put(u)
                            html_dedup_list.append(u)

                    tpl_html = str(soup.prettify())
                    self.downloaded_html_url.append((save_file_path, tpl_file_name, url))  # 存储这个3元组，最后替换html页面里的地址
                    await self.__async_save_text_file(str(tpl_html), save_file_path)
                    self.__set_dup_url(url, save_file_path)  # 用于去重
                else:
                    self.logger.error("async get error >> %s ", url)
                    self.__set_dup_url(url, tpl_file_name)
                    self.__log_error_resource(url, tpl_file_name)

            i += 1
            try:
                url = self.html_link_queue.get(timeout=1)
            except Empty as e:
                break

        self.logger.info("html页面分析完毕")
        self.__quit_cmd_enqueue()  # 没有新的url产生了
        self.__wait_unitl_task_finished()  # 这个时候异步请求也全部落到磁盘上了
        #self.download_queue.join()
        await self.__html_content_link_2_local()  # 调整html模版（磁盘）上的link为本地的地址
        await self.__make_single_page()  # 调整为单张页面
        await self.__make_web_framework_template()  # 根据用户设置转化为支持的web框架模版
        await self.__make_report()

        zip_full_path = self.__get_zip_full_path()
        self.__make_zip(zip_full_path)
        template_zip_file = self.__get_zip_relative_path(zip_full_path)
        self.zip_result_file = template_zip_file
        return template_zip_file

    async def __make_single_page(self):
        """
        把本地磁盘上的东西调整为单张页面，这种页面适合从email里发送出去
        调整算法：
        - css压缩进html
        - 然后css图片、字体压缩进css文件,
        - import 的css TODO
        - js文件再压缩进html
        - 图片（style内联以及<image标签）
        :return:
        """
        if self.is_to_single_page is False:
            return

        def __find_css_link(tag):
            return tag.name == 'link' and tag.has_attr("rel") and 'stylesheet' in tag.get('rel')

        for disk_path, file_name, url in self.downloaded_html_url:
            html_file = disk_path
            async with aiofiles.open(html_file, 'r', encoding='utf-8') as f:
                html_content = await f.read()
            soup = BeautifulSoup(html_content, 'lxml')

            ## 压缩css进html
            css_links = soup.find_all(
                __find_css_link)  # <link rel="stylesheet" href="_static/default.css" type="text/css" />
            if css_links:
                for css_el in css_links:
                    href = css_el.get("href")
                    if href and href.startswith("http"):
                        continue
                    css_el_f = f'{self.__get_tpl_full_path()}/{href}'
                    try:
                        async with aiofiles.open(css_el_f, 'r', encoding='utf-8') as f2:
                            css_content = await f2.read()

                        files = re.findall("url\(.*?\)", css_content)
                        for r in files:
                            css_resource_relative = self.__get_style_url_link(r)
                            if is_inline_resource(css_resource_relative):
                                continue
                            b64_data, type = await base64_encode_resource(self.__get_css_full_path(),
                                                                          css_resource_relative)
                            data = f"url('data:{type};charset=utf-8;base64,{b64_data}')"
                            css_content = css_content.replace(r, data)

                        css_new_tag = soup.new_tag("style", type='text/css')
                        css_new_tag.append(css_content)
                        css_el.insert_after(css_new_tag)
                        css_el.decompose()
                    except:
                        continue

            ## 压缩js进html
            def __find_js_ref(tag):
                return tag.name == 'script' and tag.has_attr("src")

            js_els = soup.find_all(__find_js_ref)
            if js_els:
                for js_el in js_els:
                    src = js_el.get("src")
                    if src and src.startswith("http"):
                        continue
                    js_el_f = f'{self.__get_tpl_full_path()}/{src}'
                    try:
                        async with aiofiles.open(js_el_f, 'r', encoding='utf-8') as f3:
                            js_content = await f3.read()
                            js_new_tag = soup.new_tag("script", type='text/javascript')
                            js_new_tag.append(js_content)
                            js_el.insert_after(js_new_tag)
                            js_el.decompose()
                    except:
                        pass

            ## 图片标签压缩进html
            img_els = soup.find_all("img")
            if img_els:
                for img in img_els:
                    img_el_f = img.get("src")
                    if img_el_f and img_el_f.startswith("http"):
                        continue
                    b64_data, type = await base64_encode_resource(self.__get_tpl_full_path(),
                                                                  img_el_f)
                    data = f"data:{type};charset=utf-8;base64,{b64_data}"
                    img['src'] = data

            ## 内联 url()压缩进html
            inner_style_node = soup.find_all(style=re.compile("url(.*?)"))  # TODO url/URL 大小写
            for style in inner_style_node:
                resource_url = re.findall('url\(.*?\)', style.get("style"))[0]  # TODO 遍历而非取第一个，匹配到全部
                resource_url = self.__get_style_url_link(resource_url)
                if is_inline_resource(resource_url):  # 内嵌base64图片
                    continue
                b64_data, type = await base64_encode_resource(self.__get_tpl_full_path(), resource_url)
                data = f"data:{type};charset=utf-8;base64,{b64_data}"
                style['style'] = style['style'].replace(resource_url, data)

            single_page = f'{html_file}.single.html'

            async with aiofiles.open(single_page, 'w', encoding='utf-8') as f_single:
                await  f_single.writelines(soup.prettify())
                self.single_page.append(f'{file_name}.single.html')

    async def __make_web_framework_template(self):
        if self.framework_support is None:
            return

        def __find_css_link(tag):
            return tag.name == 'link' and tag.has_attr("rel") and 'stylesheet' in tag.get('rel')

        for disk_path, file_name, url in self.downloaded_html_url:
            html_file = disk_path
            async with aiofiles.open(html_file, 'r', encoding='utf-8') as f:
                html_content = await f.read()
            soup = BeautifulSoup(html_content, 'lxml')

            ## link
            css_links = soup.find_all(
                __find_css_link)  # <link rel="stylesheet" href="_static/default.css" type="text/css" />
            if css_links:
                for css_el in css_links:
                    href = css_el.get("href")
                    if href and href.startswith("http"):
                        continue
                    css_el_href = f"{{% static '{href}' %}}"
                    css_el['href'] = css_el_href

            ## js
            def __find_js_ref(tag):
                return tag.name == 'script' and tag.has_attr("src")

            js_els = soup.find_all(__find_js_ref)
            if js_els:
                for js_el in js_els:
                    src = js_el.get("src")
                    if src and src.startswith("http"):
                        continue
                    js_el_src = f"""{{% static '{src}' %}}"""
                    js_el['src'] = js_el_src

            ## <image>
            img_els = soup.find_all("img")
            if img_els:
                for img in img_els:
                    img_el_f = img.get("src")
                    if img_el_f and img_el_f.startswith("http"):
                        continue
                    img_src = f"""{{% static '{img_el_f}' %}}"""
                    img['src'] = img_src

            ## 内联 url()压缩进html
            inner_style_node = soup.find_all(style=re.compile("url(.*?)"))  # TODO url/URL 大小写
            for style in inner_style_node:
                resource_url = re.findall('url\(.*?\)', style.get("style"))[0]  # TODO 遍历而非取第一个，匹配到全部
                resource_url = self.__get_style_url_link(resource_url)
                if is_inline_resource(resource_url):  # 内嵌base64图片
                    continue
                inline_url = f"""{{% static '{resource_url}' %}}"""
                style['style'] = style['style'].replace(resource_url, inline_url)

            single_page = f'{html_file}.{self.framework_support}.html'
            async with aiofiles.open(single_page, 'w', encoding='utf-8') as f_single:
                await  f_single.writelines(f"{{% load static %}}\n{soup.prettify()}")
                self.single_page.append(f'{file_name}.single.html')

    async def __html_content_link_2_local(self):
        """
        本地磁盘上的html文件里的链接修正为本地的相对路径
        :return:
        """
        abs_url_2_local_map = {}
        for save_path, file_name, url in self.downloaded_html_url:
            abs_url_2_local_map[url] = file_name

        for save_path, file_name, url in self.downloaded_html_url:
            async with aiofiles.open(save_path, "r", encoding="utf-8") as f:
                html = await f.read()
                soup = BeautifulSoup(html, "lxml")
                a_list = soup.find_all("a")
                for a in a_list:
                    raw_u = a.get("href")
                    if raw_u is not None:
                        disk_u = abs_url_2_local_map.get(raw_u)
                        if disk_u is not None:
                            a['href'] = disk_u
            async with aiofiles.open(save_path, "w", encoding="utf-8") as f:
                await f.write(soup.prettify())

    def __get_same_site_link(self, soup, url):
        """
        获取soup里的全部页面的url
        :param soup:
        :param url:
        :return:
        """
        new_url = []
        if self.is_full_site:
            a_list = soup.find_all("a")
            if a_list is not None:
                for a in a_list:
                    try:
                        raw_link = a.get("href")
                        if raw_link is None or raw_link.startswith("#") or not is_page_url(raw_link):
                            continue

                        abs_link = get_abs_url(url, raw_link)  # 新产生的url
                        abs_link = format_url(abs_link)
                        a['href'] = abs_link

                        if is_page_url(abs_link) and not is_img_ext(abs_link) and abs_link not in new_url:
                            new_url.append(abs_link)
                    except Exception as e:
                        self.logger.info("%s: %s", a, e)
                        self.logger.exception(e)
                        continue

        return new_url

    def __download_thread(self):
        """
        这个函数会在一个新的线程里协程执行
        :return:
        """
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        tasks = [asyncio.ensure_future(self.__async_download_url(), loop=loop),
                 asyncio.ensure_future(self.__async_download_url(), loop=loop),
                 asyncio.ensure_future(self.__async_download_url(), loop=loop),
                 asyncio.ensure_future(self.__async_download_url(), loop=loop),
                 ]
        loop.run_until_complete(asyncio.gather(*tasks))
        loop.close()
        self.task_finished = True

    async def __async_dl_and_save(self, url, file_save_path, file_type):
        """

        :param url:
        :return:
        """
        max_retry = config.max_retry
        time_out = config.http_timeout
        is_succ = False
        for i in range(1, max_retry + 1):
            to = time_out * i
            try:
                self.logger.info("async craw[%s] %s, file_type: %s", i, url, file_type)
                is_succ = await self.__async_spider_get(url, self.header, file_save_path, file_type, to)
                if not is_succ:
                    continue
                else:
                    return is_succ
            except asyncio.TimeoutError as te:
                self.logger.error("async retry[%s] asyncio.TimeoutError:", i)
                if i < max_retry:
                    continue
            except Exception as e:
                self.logger.error("async retry[%s] error: %s", i, e)
                self.logger.exception(e)
                if i < max_retry:
                    self.logger.info("async retry craw[%s] %s" % (i + 1, url))
                    continue

        return is_succ  # 这个地方不能删除，如果返回False, 上层会记录错误的抓取并最终体现在report里

    async def __async_spider_get(self, url, header, file_save_path, file_type='bin', to=10):
        async with aiohttp.ClientSession() as session:
            is_succ = await self.__do_download(session, url, header, file_save_path, file_type, to)
            return is_succ

    async def __do_download(self, session, url, header, file_save_path, file_type, to):
        try:
            async with session.get(url, timeout=to, headers=header) as response:
                if response.status != 200:
                    return False

                if file_type == self.FILE_TYPE_TEXT:
                    text = await response.text()
                    await self.__async_save_text_file(text, file_save_path)
                else:
                    async with aiofiles.open(file_save_path, 'wb') as fd:
                        while True:
                            chunk = await response.content.read(512)
                            if not chunk:
                                break
                            else:
                                await fd.write(chunk)
                return True
        except Exception as e:
            return False

    async def __async_download_url(self):
        """

        :return:
        """
        quit_wait_times = 5  # 当收到quit命令时，还要再等待一会
        try:
            while True:
                if self.download_finished is True:
                    break
                cmd = self.download_queue.get()
                if not cmd:  # 超时没拿到东西，让出cpu然后再来拿
                    self.logger.info("queue get nothing")
                    await asyncio.sleep(config.url_download_queue_timeout)
                    continue

                self.logger.debug("queue get cmd %s", cmd)
                cmd_content = cmd['cmd']
                if cmd_content == self.CMD_QUIT:  # 虽然这里收到了，但是不能排除其他协程还在等待网络返回,还在向队列里新增url
                    self.logger.info("quit download task")
                    self.download_finished = True
                    #self.download_queue.task_done()
                    if quit_wait_times>0:
                        quit_wait_times -= 1
                        await asyncio.sleep(1)
                        continue
                    else:
                        break  # 收到最后一条命令，退出。任务结束
                else:
                    url = cmd['url']
                    save_path = cmd['file_save_path']
                    file_type = cmd['file_type']
                    try:
                        is_succ = await self.__async_dl_and_save(url, save_path, file_type)
                    except asyncio.exceptions.CancelledError as ce:
                        logger.error(ce)
                        is_succ = False
                    if is_succ is False:
                        self.logger.error("async get %s error", url)
                        self.__log_error_resource(url, self.__get_relative_report_file_path(save_path))
                        self.__set_dup_url(url, save_path)
                    else:
                        self.__set_dup_url(url, save_path)
        except Exception as e:
            logger.exception(e)
            pass
        except asyncio.exceptions.CancelledError as b:
            logger.error(b)
            pass

    def __make_zip(self, zip_full_path):
        shutil.make_archive(zip_full_path, 'zip', self.download_temp_abs_dir, base_dir=self.__get_tpl_dir())
        self.logger.info(f"zip file {zip_full_path}.zip make ok")


if __name__ == "__main__":
    fileConfig('logging.ini')
    """
    动态渲染的： 'https://docs.python.org/3/library/os.html',http://www.gd-n-tax.gov.cn/gdsw/index.shtml
    需要UA：'https://stackoverflow.com/questions/13137817/how-to-download-image-using-requests',
    gb2312 : https://www.jb51.net/web/25623.html
    import css  https://templated.co/items/demos/intensify/index.html
    https://prium.github.io/falcon/
    """
    url_list = [
        "http://www.qdujava.com",
    ]
    n1 = datetime.now()
    spider = TemplateCrawler(url_list, save_base_dir="/home/cxu/spider-template/",
                             header={'User-Agent': config.default_ua}, grab_out_site_link=True, to_single_page=False,
                             full_site=True, ref_model=False, framework=None)

#    spider = TemplateCrawler(url_list, save_base_dir="/home/cxu/spider-template/",
#                             header={'User-Agent': config.default_ua},
#                             grab_out_site_link=True, to_single_page=True, full_site=False, ref_model=False,
#                             framework=None)

asyncio.run(spider.template_crawl())
n2 = datetime.now()
print(n2 - n1)
