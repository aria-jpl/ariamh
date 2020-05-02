import os
import isce_functions_alos2







def main():
    md = isce_functions_alos2.get_alos2_metadata("/sp_data/alos2/david/reference")
    print(md)

if __name__ == '__main__':

    main()
