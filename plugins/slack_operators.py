
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

from airflow.operators.slack_operator import SlackAPIOperator, SlackAPIPostOperator

from airflow.exceptions import AirflowException, AirflowSkipException

from slackclient import SlackClient

import ast
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

    def execute(self, context):
        if not self.api_params:
            self.construct_api_call_params()
        sc = SlackClient(self.token)
        rc = sc.api_call(self.method, **self.api_params)
        if not rc['ok']:
            if not rc['error'] == 'name_taken':
                logging.error("Slack API call failed ({})".format(rc['error']))
                raise AirflowException("Slack API call failed: ({})".format(rc['error']))
        rc = sc.api_call('groups.list', **{ **self.api_params, **{'exclude_archived': 1} } )
        group = list( filter( lambda d: d['name'] in [ self.channel.lower(), ], rc['groups'] ) )[0]
        context['task_instance'].xcom_push( key='return_value', value={
            'group_id': group['id'],
            'members': group['members']
        } )

def user_to_slack_id( user ):
    mapping = {
        #'alpays': 'W9RNLGK46',
        '15453': 'W9RNLGK46',
        #'donghuac': 'W9RNLFXCN',
        '15108': 'W9RNLFXCN',
        #'bushnell': 'W9QJSF0E5',
        '12926': 'W9QJSF0E5',
        # 'hongli': 'W9RUM0VM5',
        '15547': 'W9RUM0VM5',
        #'kmzhang': 'W9QTNMG9G',
        '15319': 'W9QTNMG9G',
        #'djchmiel': 'W9XMPLUBA',
        '15109': 'W9XMPLUBA',
        #'qqh': 'W9QT49P6E',
        '15664': 'W9QT49P6E',
        'cgati': 'W9Q7ULA9E',
        #'megmayer': 'WAM16PCR2',
        '15669': 'WAM16PCR2',
        #'sroh': 'W9QS8L6S0',
        '15187': 'W9QS8L6S0',
        #'boxue': 'W9QSBDSSG',
        '15309': 'W9QSBDSSG',
        #'weijianz': 'W9QQ79B1R',
        '15528': 'W9QQ79B1R',
        #'yanyanz': 'W9QS8L3GU',
        '15539': 'W9QS8L3GU',
        #'yamuna': 'W9RNLGC06',
        '15386': 'W9RNLGC06',
        # cxiao
        '15734': 'W9RSJ22HX',
        # biofeng
        '15818': 'W9TC6LFB3',
        # suzm
        '15195': 'W9QT49XGA',
        # iwhite
        '14908': 'WBU3NCL1M',
        # jleitz
        '16046': 'WAMEHEQ77',
        # zhouq
        '16045': 'WCLLQL154',
        # mcmorais
        '15761': 'W9QT4ALR0',
        # eam
        '16174': 'WCPH4JZFU',
        # jjin0913
        #'15790': 
        # jgalaz
        '15346': 'WAT2E757A',
        # wukon
        '15321': 'W9QAU6YMP',
        # minru fan
        #'WD3GVPX99',
        # wei huang
        '15801': 'W9RUM1W3Z',
    }
    if user in mapping:
        return mapping[user]
    # is already a slack user id
    if user.startswith('W'):
        return user
    return None

        
class SlackAPIInviteToChannelOperator(SlackAPIOperator):
    template_fields = ('channel','users')
    ui_color = '#FFBA40'

    @apply_defaults
    def __init__(self,
                 channel='#general',
                 users=(),
                 default_users=None,
                 *args, **kwargs):
        self.method = 'groups.invite'
        self.channel = channel
        self.users = users #ast.literal_eval(users)
        self.default_users = default_users.split(',')
        super(SlackAPIInviteToChannelOperator, self).__init__(method=self.method,
                                                   *args, **kwargs)

    def construct_api_call_params(self, channel_id=None ):
        self.api_params = {
            'channel': channel_id,
        } 

    def execute(self, context):
        current = context['task_instance'].xcom_pull( task_ids='slack_channel' )
        if not self.api_params:
            self.construct_api_call_params( channel_id=current['group_id'] )
        sc = SlackClient(self.token)
        these = ast.literal_eval( "%s" % (self.users,) )
        #logging.info("CURRENT: %s" % (current,))
        these = [ user_to_slack_id(u) for u in these + self.default_users if not u in current['members'] ] 
        logging.info("THESE: %s" % (these,))
        err = []
        for u in these:
            self.api_params.update( { 'user': u } )
            #logging.warn("groups.invite params: %s" % (self.api_params,))
            rc = sc.api_call(self.method, **self.api_params)
            if not rc['ok']:
                logging.error("Slack API call failed ({})".format(rc['error']))
                err.append( ( u, rc['error'] ) )
        if len(err):
            raise AirflowException("Slack API call failed for user %s: %s" % (err[0][0],err[0][1]) )

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
            logging.info("sending: %s" % (self.api_params,))
            if not rc['ok']:
                logging.error("Slack API call failed {}".format(rc['error']))
                raise AirflowException("Slack API call failed: {}".format(rc['error']))




class SlackPlugin(AirflowPlugin):
    name = 'slack_plugin'
    operators = [ SlackAPIEnsureChannelOperator, SlackAPIInviteToChannelOperator, SlackAPIUploadFileOperator ]
