import logging
import ckan.lib.cli
import sys

from ckanext.swisstopo.helpers import s3

class MeteoswissCommand(ckan.lib.cli.CkanCommand):
    '''Command to handle swisstopo data

    Usage:

        # Show this help
        paster --plugin=ckanext-swisstopo swisstopo help -c <path to config file>

        # Import datasets
        paster --plugin=ckanext-swisstopo swisstopo import -c <path to config file>

        # List all files in the S3 bucket
        paster --plugin=ckanext-swisstopo swisstopo list -c <path to config file>

        # Show output from CSW, 'query' is typically the name of a dataset like 'swissboundaries3D'
        paster --plugin=ckanext-swisstopo swisstopo csw <query> -c <path to config file>

    '''
    summary = __doc__.split('\n')[0]
    usage = __doc__

    def command(self):
        # load pylons config
        self._load_config()
        options = {
                'list': self.listCmd,
                'help': self.helpCmd,
        }

        try:
            cmd = self.args[0]
            options[cmd](*self.args[1:])
        except KeyError:
            self.helpCmd()
            sys.exit(1)

    def helpCmd(self):
        print self.__doc__

    def listCmd(self):
        s3_helper = s3.S3();
        for file in s3_helper.list():
            print file