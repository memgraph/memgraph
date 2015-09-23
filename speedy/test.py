import requests

endpoint = 'http://localhost:3400/test%s'
methods = [('GET', requests.get), ('POST', requests.post), ('PUT', requests.put), ('DELETE', requests.delete)]

print ''
isAllFine = True
for index in range(1, 4):
    for name, method in methods:
        url = endpoint % index
        r = method(url)
        if r.status_code == 200 and r.text == 'test':
            print name, url, 'PASS'
        else:
            print name, url, 'FAIL'
            isAllFine = False
print ''
if isAllFine:
    print 'Great. All tests have passed :)'
else:
    print 'Fuckup. Something went wrong!'
print ''
