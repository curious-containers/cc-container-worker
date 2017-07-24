import resource
import seccomplite


class Sandbox:
    SANDBOX_MODE_DISABLED = 'disabled'
    SANDBOX_MODE_WHITELIST = 'whitelist'
    SANDBOX_MODE_BLACKLIST = 'blacklist'
    SANDBOX_MODE_DEFAULT = SANDBOX_MODE_DISABLED

    LIMITS = {'cpu_usage': resource.RLIMIT_CPU,
              'create_file_size': resource.RLIMIT_FSIZE,
              'num_open_files': resource.RLIMIT_NOFILE,
              'heap_size': resource.RLIMIT_DATA,
              'stack_size': resource.RLIMIT_STACK,
              'rss_size': resource.RLIMIT_RSS,
              'child_processes': resource.RLIMIT_NPROC
              }

    OPERATOR_MAP = {
        '==': seccomplite.EQ,
        '!=': seccomplite.NE,
        '>=': seccomplite.GE,
        '>': seccomplite.GT,
        '<=': seccomplite.LE,
        '<': seccomplite.LT,
        '&=': seccomplite.MASKED_EQ
    }

    def __init__(self, config, termination_exit_code=1234):
        self.config = config
        self.termination_exit_code = termination_exit_code

    def enter(self):
        # Establish resource limits if requested
        if self.config and self.config.get('limits', None):
            Sandbox._load_limits(self.config['limits'])

        if self.config and self.config.get('seccomp', None):
            Sandbox._load_seccomp(self.config['seccomp'], self.termination_exit_code)

    @staticmethod
    def _load_limits(limits_config):
        for key in Sandbox.LIMITS.keys():
            if limits_config.get(key, None):
                limit = limits_config[key]
                option = Sandbox.LIMITS[key]
                resource.setrlimit(option, (limit, limit))

    @staticmethod
    def _load_seccomp(sandbox_config, termination_exit_code):
        # Check if sandboxing is requested
        mode = sandbox_config.get('mode', Sandbox.SANDBOX_MODE_DEFAULT)
        if mode == Sandbox.SANDBOX_MODE_DISABLED:
            return

        # Create the sandbox handler
        default_action = seccomplite.ERRNO(termination_exit_code)
        rule_action = seccomplite.ALLOW
        if mode == Sandbox.SANDBOX_MODE_BLACKLIST:
            default_action, rule_action = rule_action, default_action

        seccomp_filter = seccomplite.Filter(default_action)
        architecture = seccomplite.Arch(seccomplite.Arch.NATIVE)
        if not seccomp_filter.exist_arch(architecture):
            seccomp_filter.add_arch(architecture)

        # Process termination and return on signal are always allowed
        if mode == Sandbox.SANDBOX_MODE_WHITELIST:
            seccomp_filter.add_rule(seccomplite.ALLOW, "exit", )
            seccomp_filter.add_rule(seccomplite.ALLOW, "exit_group")
            seccomp_filter.add_rule(seccomplite.ALLOW, "rt_sigreturn")

        for filter_item_config in sandbox_config['filter_items']:
            syscall = filter_item_config['syscall']
            conditions = Sandbox._make_conditions(filter_item_config)
            seccomp_filter.add_rule(rule_action, syscall, conditions)

        seccomp_filter.load()

    @staticmethod
    def _make_conditions(filter_item_config):
        result = []
        for item in filter_item_config.get('conditions', []):
            result.append(Sandbox._make_condition(item))

        return result

    @staticmethod
    def _make_condition(condition_config):
        argument = condition_config['argument']
        operator = Sandbox.OPERATOR_MAP.get(condition_config['operator'])
        datum_a = condition_config['datum_a']
        datum_b = condition_config.get('datum_b', 0)

        return seccomplite.Arg(argument, operator, datum_a, datum_b)

