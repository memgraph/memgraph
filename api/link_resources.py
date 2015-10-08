# this script generates the include.hpp file for restful resources
import re
import os

print "generating include.hpp file"

resource_path = 'resources'
template_path = 'resources/include.hpp.template'
include_path = 'resources/include.hpp'

# remove the old version of the file if exists
if os.path.isfile(include_path):
    os.remove(include_path)


class Resource(object):
    """ represents a restful resource class for speedy """

    def __init__(self, filename):
        self.filename = filename
        self.class_name = None

        with open(os.path.join(resource_path, filename)) as f:
            class_name = re.compile('\s*class\s*(\w+)\s*\:')

            for line in f:
                result = re.search(class_name, line)

                if result is None:
                    continue

                self.class_name = result.group(1)

                break


def load_resources():
    resources = [Resource(f) for f in os.listdir(resource_path)
                 if f.endswith('.hpp')]

    return [r for r in resources if r.class_name is not None]


def write_includes(file, resources):
    for filename in [resource.filename for resource in resources]:
        print 'writing include for', filename
        file.write('#include "{}"\n'.format(filename))


def write_inits(file, resources):
    for class_name in [resource.class_name for resource in resources]:
        print 'writing init for', class_name
        file.write('    insert<{}>(app);\n'.format(class_name))


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
