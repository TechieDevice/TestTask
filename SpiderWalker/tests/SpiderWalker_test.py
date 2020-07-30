import pytest
from SpiderWalker import parse_data


class TestParsing:
    def test_ParseData_SearchLinksInHTML_ReturnLinks(self):
        with open('./tests/rawHTML.html', 'r') as rawHTML:
            html_data = rawHTML.read()
        with open('./tests/rawLinks.txt', 'r') as rawLinks:
            links = []
            for line in rawLinks:
                if line != 'None\n':
                    links.append(line[:-1])
                else:
                    links.append(None)
        assert parse_data(html_data) == links
