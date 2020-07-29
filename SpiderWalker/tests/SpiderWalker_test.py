import pytest
from SpiderWalker import parse_data


class TestParsing:
    def test_ParseData_SerchLinksInHTML_Links(self):
        rawHTML = open(f'E:/User/Desktop/test_app/git/TestTask/SpiderWalker/tests/rawHTML.html', 'r')
        html_data = rawHTML.read()
        rawHTML.close()
        rawLinks = open(f'E:/User/Desktop/test_app/git/TestTask/SpiderWalker/tests/rawLinks.txt', 'r')
        links = []
        for line in rawLinks:
            if line != 'None\n':
                links.append(line[:-1])
            else:
                links.append(None)
        rawLinks.close()
        assert parse_data(html_data) == links
