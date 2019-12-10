
import sys
sys.path.append('.')

from interferogram.sentinel.create_standard_product_s1 import get_polarization


def test_get_gateway_ip():
    reference_slc_id = "S1A_IW_SLC__1SDV_20190809T231443_20190809T231511_028499_0338B3_C446"
    ref_pol = get_polarization(reference_slc_id)
    print(ref_pol)
    assert ref_pol == "vv"
