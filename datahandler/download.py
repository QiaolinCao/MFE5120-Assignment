from handler import DataHandler

handler = DataHandler("ashare")
symbols = handler.get_all_ashare_stock_symbol()
handler.download_ashare_bar_data_to_database(symbols, "20130101", "20231231")
