from datahandler import DataHandler

hdl = DataHandler("ashare")

hdl.load_ashare_bar_data("all", "d", "20190101", "20231231")
df = hdl.get_bar_datas_df()

df.to_csv("ashare_bar_data.csv")