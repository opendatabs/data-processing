import logging
import pandas as pd
import xml
from xml.sax.handler import ContentHandler
from parlamentsdienst_gr_abstimmungen import credentials


# see https://stackoverflow.com/a/33504236
class ExcelHandler(ContentHandler):
    def __init__(self):
        super().__init__()
        self.chars = []
        self.cells = []
        self.rows = []
        self.tables = []

    def characters(self, content):
        self.chars.append(content)

    def startElement(self, name, atts):
        if name == "Cell":
            self.chars = []
        elif name == "Row":
            self.cells = []
        elif name == "Table":
            self.rows = []

    def endElement(self, name):
        if name == "Cell":
            self.cells.append(''.join(self.chars))
        elif name == "Row":
            self.rows.append(self.cells)
        elif name == "Table":
            self.tables.append(self.rows)


def main():
    excel_handler = ExcelHandler()
    xml.sax.parse(credentials.file_name, excel_handler)
    polls = pd.DataFrame(excel_handler.tables[1][1:], columns=excel_handler.tables[1][0])
    details = pd.DataFrame(excel_handler.tables[0][1:101], columns=excel_handler.tables[0][0])
    sums_per_decision = pd.DataFrame(excel_handler.tables[0][101:107], columns=excel_handler.tables[0][0])
    timestamp = excel_handler.tables[0][108][1]
    pass


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
