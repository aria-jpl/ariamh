import os
import isce_functions_alos2







def main():
    md = isce_functions_alos2.create_alos2_md_file("/data/SP1/secondary", "sec_alos2_md.json")
    print(md)

if __name__ == '__main__':

    main()
