from charms.reactive import when, when_not, set_state, is_state, remove_state
from charms.hadoop import get_hadoop_base
from jujubigdata.handlers import YARN
from jujubigdata import utils
from charmhelpers.core import hookenv


@when('hadoop.installed')
@when_not('resourcemanager.related')
def blocked():
    hookenv.status_set('blocked', 'Waiting for relation to ResourceManager')


@when('hadoop.installed', 'resourcemanager.related')
def set_spec(resourcemanager):
    hadoop = get_hadoop_base()
    resourcemanager.set_nodemanager_spec(hadoop.spec())


@when('resourcemanager.spec.mismatch')
def spec_mismatch(resourcemanager):
    hookenv.status_set('blocked',
                       'Spec mismatch with ResourceManager: {} != {}'.format(
                           resourcemanager.nodemanager_spec(), resourcemanager.resourcemanager_spec()))


@when('hadoop.installed', 'resourcemanager.related')
@when_not('resourcemanager.spec.mismatch', 'resourcemanager.ready', 'nodemanager.started')
def waiting(resourcemanager):
    hookenv.status_set('waiting', 'Waiting for ResourceManager')


@when('resourcemanager.ready')
@when_not('nodemanager.started')
def start_nodemanager(resourcemanager):
    hadoop = get_hadoop_base()
    yarn = YARN(hadoop)
    yarn.configure_nodemanager(resourcemanager.host(), resourcemanager.port())
    utils.install_ssh_key('ubuntu', resourcemanager.ssh_key())
    yarn.start_nodemanager()
    resourcemanager.register()
    hadoop.open_ports('nodemanager')
    set_state('nodemanager.started')
    hookenv.status_set('active', 'Ready')


@when('nodemanager.started')
@when_not('resourcemanager.ready')
def stop_nodemanager():
    hadoop = get_hadoop_base()
    yarn = YARN(hadoop)
    yarn.stop_nodemanager()
    hadoop.close_ports('nodemanager')
    remove_state('nodemanager.started')

#-----------
#
#@when_not('spec.mismatch', 'resourcemanager.registered', 'nodemanager.started')
#def mark_blocked_waiting():
#    from charms.reactive.bus import get_states
#    hookenv.log('States: {}'.format(get_states().keys()))
#    if not is_state('resourcemanager.related'):
#        hookenv.status_set('blocked', 'Waiting for relation to ResourceManager')
#    else:
#        hookenv.status_set('waiting', 'Waiting for ResourceManager')
#
#
#@when_not('resourcemanager.available')
#def no_spec():
#    # unavailable spec can neither be confirmed nor denied
#    remove_state('spec.verified')
#    remove_state('spec.mismatch')
#
#
#@when('resourcemanager.available')
#def verify_spec(resourcemanager):
#    hadoop = get_hadoop_base()
#    if utils.spec_matches(hadoop.spec(), resourcemanager.spec()):
#        set_state('spec.verified')
#        remove_state('spec.mismatch')
#    else:
#        hookenv.log('Spec mismatch: {} != {}'.format(hadoop.spec(), resourcemanager.spec()))
#        hookenv.status_set('blocked',
#                           'Spec mismatch with ResourceManager: {} != {}'.format(
#                               hadoop.spec(), resourcemanager.spec()))
#        remove_state('spec.verified')
#        set_state('spec.mismatch')
#
#
#@when('spec.verified', 'resourcemanager.available')
#def update_etc_hosts(resourcemanager):
#    utils.update_kv_hosts(resourcemanager.hosts_map())
#    utils.manage_etc_hosts()
#
#
#@when('spec.verified', 'resourcemanager.available')
#@when_not('resourcemanager.registered')
#def register_nodemanager(resourcemanager):
#    resourcemanager.register_nodemanager()
#
#
#@when('spec.verified', 'resourcemanager.registered')
#@when_not('nodemanager.started')
#def start_nodemanager(resourcemanager):
#    hadoop = get_hadoop_base()
#    yarn = YARN(hadoop)
#    yarn.configure_nodemanager(resourcemanager.host(), resourcemanager.port())
#    yarn.start_nodemanager()
#    hadoop.open_ports('nodemanager')
#    set_state('nodemanager.started')
#
#
#@when_not('spec.verified', 'resourcemanager.available')
#def stop_nodemanager():
#    if is_state('nodemanager.started'):
#        hadoop = get_hadoop_base()
#        yarn = YARN(hadoop)
#        yarn.stop_nodemanager()
#        hadoop.close_ports('nodemanager')
#        remove_state('nodemanager.started')
#
#
#@when('nodemanager.started')
#def mark_active():
#    hookenv.status_set('active', 'Ready')
