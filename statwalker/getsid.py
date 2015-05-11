import ctypes
from ctypes import wintypes

_PSECURITY_DESCRIPTOR = ctypes.POINTER(wintypes.BYTE)
_PSID = ctypes.POINTER(wintypes.BYTE)
_LPDWORD = ctypes.POINTER(wintypes.DWORD)
_LPBOOL = ctypes.POINTER(wintypes.BOOL)
_LPCSTR = ctypes.POINTER(ctypes.c_char_p)
_OWNER_SECURITY_INFORMATION = 0X00000001

_advapi32 = ctypes.windll.advapi32

##MSDN windows/desktop/aa446639
_GetFileSecurity = _advapi32.GetFileSecurityW
_GetFileSecurity.restype = wintypes.BOOL
_GetFileSecurity.argtypes = [
    wintypes.LPCWSTR,      #File Name (in)
    wintypes.DWORD,        #Requested Information (in)
    _PSECURITY_DESCRIPTOR, #Security Descriptor (out_opt)
    wintypes.DWORD,        #Length (in)
    _LPDWORD,              #Length Needed (out)
]

##MSDN windows/desktop/aa446651
_GetSecurityDescriptorOwner = _advapi32.GetSecurityDescriptorOwner
_GetSecurityDescriptorOwner.restype = wintypes.BOOL
_GetSecurityDescriptorOwner.argtypes = [
    _PSECURITY_DESCRIPTOR,  #Security Descriptor (in)
    ctypes.POINTER(_PSID),  #Owner (out)
    _LPBOOL,                #Owner Exists (out)
]

##MSDN windows/desktop/aa379166
_LookupAccountSid = _advapi32.LookupAccountSidW
_LookupAccountSid.restype = wintypes.BOOL
_LookupAccountSid.argtypes = [
    wintypes.LPCWSTR, #System Name (in)
    _PSID,            #SID (in)
    wintypes.LPCWSTR, #Name (out)
    _LPDWORD,         #Name Size (inout)
    wintypes.LPCWSTR, #Domain(out_opt)
    _LPDWORD,         #Domain Size (inout)
    _LPDWORD,         #SID Type (out)
]

_ConvertSidToStringSid = _advapi32.ConvertSidToStringSidA
_ConvertSidToStringSid.restype = wintypes.BOOL
_ConvertSidToStringSid.argtypes = [
    _PSID,          #Sid (in)
    _LPCSTR        #StringSid (out)
]

# S-1-5-21-1810871202-3888432777-2958109185-1001
def get_sid(filename):
    length = wintypes.DWORD()
    # _GetFileSecurity(filename, _OWNER_SECURITY_INFORMATION, None, 0, ctypes.byref(length))
    # if not length.value:
    #     return None
    # print (length.value)
    length.value = 48
    sd = (wintypes.BYTE * length.value)()
    if not _GetFileSecurity(filename, _OWNER_SECURITY_INFORMATION, sd, length, ctypes.byref(length)):
        return None
    if not sd:
        print('Error: %s security descriptor is null, does not fit in %s bytes' % (filename, length.value))
        return None
    sid = _PSID()
    sid_defaulted = wintypes.BOOL()
    if not _GetSecurityDescriptorOwner(sd, ctypes.byref(sid), ctypes.byref(sid_defaulted)):
        return None
    SIZE = 48
    ssid = ctypes.create_string_buffer(SIZE)
    pssid = ctypes.c_char_p(ctypes.addressof(ssid))
    if _ConvertSidToStringSid(sid, ctypes.byref(pssid)):
        return pssid.value
    return None


if __name__ == '__main__':
    import sys

    filename = sys.argv[1]
    sid = get_sid(filename)

    if sid is not None:
        print("sid: {0}".format(sid))
    else:
        print('Error')
