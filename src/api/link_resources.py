# this script generates the include.hpp file for restful resources
import re
import os
from itertools import chain

print "generating include.hpp file"

resource_path = 'resources'
template_path = 'resources/include.hpp.template'
include_path = 'resources/include.hpp'

# remove the old version of the file if exists
if os.path.isfile(include_path):
    os.remove(include_path)


class Resource(object):
    """ represents a restful resource class for speedy """

    def __init__(self, filename, class_name, url):
        self.filename = filename
        self.class_name = class_name
        self.url = url


def scan_resources(filename):

    with open(os.path.join(resource_path, filename)) as f:
        url_regex = re.compile('#pragma\s+url\s+([^\s]+)\s+')
        class_name_regex = re.compile('\s*class\s*(\w+)\s*\:')

        lines = f.readlines()
        pairs = zip(lines, lines[1:])

        for first, second in pairs:
            url = re.search(url_regex, first)

            if url is None:
                continue

            class_name = re.search(class_name_regex, second)

            if class_name is None:
                continue

            yield Resource(filename, class_name.group(1), url.group(1))


def load_resources():
    resources = chain(*[scan_resources(f) for f in os.listdir(resource_path)
                        if f.endswith('.hpp')])

    return [r for r in resources if r.class_name is not None]


def write_includes(file, resources):
    for filename in [resource.filename for resource in resources]:
        print 'writing include for', filename
        file.write('#include "{}"\n'.format(filename))


def write_inits(file, resources):
    for class_name, url in [(r.class_name, r.url) for r in resources]:
        print('writing init for {} -> {}'.format(class_name, url))
        file.write('    insert<{}>(container, "{}");\n'
                   .format(class_name, url))


def make_include_file():
    resources = load_resources()

    with open(template_path, 'r') as ftemplate:
        with open(include_path, 'w') as finclude:
            for line in ftemplate:
                if '<INCLUDE>' in line:
                    write_includes(finclude, resources)
                    continue

                if '<INIT>' in line:
                    write_inits(finclude, resources)
                    continue

                finclude.write(line)

if __name__ == '__main__':
    make_include_file()
