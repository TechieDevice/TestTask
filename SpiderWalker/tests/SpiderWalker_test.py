from script import SpiderWalker


class TestParsing:
    def test_parse_data_with_sdo(self):
        """parse_data(html) search links in html and return links[http://...]"""

        with open("./tests/rawHTML.html", "r") as rawHTML:
            html_data = rawHTML.read()
        with open("./tests/rawLinks.txt", "r") as rawLinks:
            links = []
            for line in rawLinks:
                if line != "None\n":
                    links.append(line[:-1])
                else:
                    links.append(None)

        result = SpiderWalker.parse_data(html_data)

        assert result == links
