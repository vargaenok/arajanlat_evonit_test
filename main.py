def input_handling():
    start = input("Starting Port: ")
    finish = input("Target City: ")
    container_type = input("Container type (20ST, 40ST, 40HC): ")
    profit_percent = float(input("Profit margin: "))
    usd_huf_change = float(input("USD/HUF exchange rate: "))
    print(start, finish, container_type, profit_percent, usd_huf_change)


if __name__ == '__main__':
    input_handling()

