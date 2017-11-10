
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

from airflow.operators.slack_operator import SlackAPIOperator, SlackAPIPostOperator

from airflow.exceptions import AirflowException, AirflowSkipException

from slackclient import SlackClient

import os
import logging

LOG = logging.getLogger(__name__)



class SlackAPIEnsureChannelOperator(SlackAPIOperator):
    template_fields = ('channel',)
    ui_color = '#FFBA40'

    @apply_defaults
    def __init__(self,
                 channel='#general',
                 *args, **kwargs):
        self.method = 'groups.create'
        self.channel = channel
        super(SlackAPIEnsureChannelOperator, self).__init__(method=self.method,
                                                   *args, **kwargs)

    def construct_api_call_params(self):
        self.api_params = {
            'name': self.channel,
            'validate': True,
        }

    def execute(self, **kwargs):
        if not self.api_params:
            self.construct_api_call_params()
        sc = SlackClient(self.token)
        rc = sc.api_call(self.method, **self.api_params)
        if not rc['ok']:
            if not rc['error'] == 'name_taken':
                logging.error("Slack API call failed ({})".format(rc['error']))
                raise AirflowException("Slack API call failed: ({})".format(rc['error']))


        
class SlackAPIInviteToChannelOperator(SlackAPIOperator):
    template_fields = ('channel','users')
    ui_color = '#FFBA40'

    @apply_defaults
    def __init__(self,
                 channel='#general',
                 users=(),
                 *args, **kwargs):
        self.method = 'groups.invite'
        self.channel = channel
        self.users = users
        super(SlackAPIInviteToChannelOperator, self).__init__(method=self.method,
                                                   *args, **kwargs)

    def construct_api_call_params(self):
        self.api_params = {
            'channel': self.channel,
        } 

    def execute(self, **kwargs):
        if not self.api_params:
            self.construct_api_call_params()
        sc = SlackClient(self.token)
        for u in self.users:
            self.api_params.update( { 'user': u } )
            logging.warn("groups.invite params: %s" % (self.api_params,))
            rc = sc.api_call(self.method, **self.api_params)
            if not rc['ok']:
                logging.error("Slack API call failed ({})".format(rc['error']))
                raise AirflowException("Slack API call failed: ({})".format(rc['error']))

class SlackAPIUploadFileOperator(SlackAPIOperator):
    template_fields = ('channel','filepath')
    ui_color = '#FFBA40'

    @apply_defaults
    def __init__(self,
                 channel='#general',
                 filepath=None,
                 *args, **kwargs):
        self.method = 'files.upload'
        self.channel = channel
        self.filepath = filepath
        super(SlackAPIUploadFileOperator, self).__init__(method=self.method,
                                                   *args, **kwargs)

    def construct_api_call_params(self):
        title = os.path.basename(self.filepath)
        self.api_params = {
            'channels': self.channel,
            'filename': title,
            'title': title,
        }

    def execute(self, **kwargs):
        if not self.api_params:
            self.construct_api_call_params()
        sc = SlackClient(self.token)
        with open( self.filepath, 'rb' ) as f:
            self.api_params['file'] = f
            rc = sc.api_call(self.method, **self.api_params)
            if not rc['ok']:
                logging.error("Slack API call failed ({})".format(rc['error']))
                raise AirflowException("Slack API call failed: ({})".format(rc['error']))




class SlackPlugin(AirflowPlugin):
    name = 'slack_plugin'
    operators = [ SlackAPIEnsureChannelOperator, SlackAPIInviteToChannelOperator, SlackAPIUploadFileOperator ]