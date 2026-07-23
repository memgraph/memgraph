# complete code
import os
import re

def update_readme():
    """
    Updates the README file with new links and information.
    """
    try:
        # Read the existing README file
        with open('README.md', 'r') as f:
            readme_content = f.read()

        # Add links to Discourse, Lima, and other resources
        links = [
            '[Discourse](https://discourse.memgraph.com/)',
            '[Lima](https://github.com/lima-vm/lima)',
            '[MacOS Subsystem for Linux](https://github.com/macbian-linux/macos-subsystem-for-linux)',
        ]

        # Add links to the README content
        for link in links:
            readme_content += f'\n{link}\n'

        # Add a new section for build status
        build_status = """
## Build Status
| Operating System | Status |
| --- | --- |
| Ubuntu | [![Ubuntu Build Status](https://github.com/memgraph/memgraph/actions/workflows/build_packages.yml/badge.svg)](https://github.com/memgraph/memgraph/actions/workflows/build_packages.yml) |
| Debian | [![Debian Build Status](https://github.com/memgraph/memgraph/actions/workflows/build_packages.yml/badge.svg)](https://github.com/memgraph/memgraph/actions/workflows/build_packages.yml) |
| Master | [![Master Build Status](https://github.com/memgraph/memgraph/actions/workflows/build_packages.yml/badge.svg)](https://github.com/memgraph/memgraph/actions/workflows/build_packages.yml) |
| Release | [![Release Build Status](https://github.com/memgraph/memgraph/actions/workflows/build_packages.yml/badge.svg)](https://github.com/memgraph/memgraph/actions/workflows/build_packages.yml) |
"""

        # Add the build status section to the README content
        readme_content += f'\n\n{build_status}'

        # Write the updated README content to the file
        with open('README.md', 'w') as f:
            f.write(readme_content)

        print('README file updated successfully.')

    except Exception as e:
        print(f'Error updating README file: {e}')

if __name__ == '__main__':
    update_readme()