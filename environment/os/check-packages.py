# complete code
import os
import re

def check_packages():
    """
    Checks if all required packages are installed.
    """
    try:
        # Check if all required packages are installed
        packages = ['gcc', 'make', 'cmake', 'conan']
        for package in packages:
            if not os.system(f'which {package}'):
                print(f'{package} installed.')
            else:
                print(f'{package} not installed.')

        print('All required packages installed.')

    except Exception as e:
        print(f'Error checking packages: {e}')

if __name__ == '__main__':
    check_packages()