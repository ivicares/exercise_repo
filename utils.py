import os
import sys
import subprocess
import shlex


def sort_out_the_channel(channel_name):
    output_to = subprocess.PIPE
    if channel_name == subprocess.DEVNULL:
        output_to = subprocess.DEVNULL
    elif type(channel_name) == str:
        output_to = open(channel_name, "a")

    return output_to


def set_env(env_variables, unset_env_vars, env_vars_extend):
    env = None
    if not (env_variables or unset_env_vars or env_vars_extend):  return env
    env = os.environ.copy()  # the inherited environment
    if env_variables:
        for env_var_name, new_value in env_variables.items():
            env[env_var_name] = new_value
    if unset_env_vars:
        for env_var_name in unset_env_vars:
            if env_var_name in env: del env[env_var_name]
    if env_vars_extend:
        for env_var_name, additional_value in env_vars_extend.items():
            if env_var_name in env:
                env[env_var_name] = f"{env[env_var_name]}:{additional_value}"
            else:
                env[env_var_name] = additional_value
    return env


########################################
def piped_subprocess(cmdlist, env_variables=None, unset_env_vars=None, env_vars_extend=None,
                     stdoutfnm=None, errorfnm=None):

    stdout_to = sort_out_the_channel(stdoutfnm)
    stderr_to = stdout_to if errorfnm == stdoutfnm else sort_out_the_channel(errorfnm)

    env  = set_env(env_variables, unset_env_vars, env_vars_extend)
    prev = subprocess.Popen(shlex.split(cmdlist[0]), stdout=subprocess.PIPE, stderr=stderr_to, env=env)
    for cmd in cmdlist[1:-1]:
        p = subprocess.Popen(shlex.split(cmd), stdin=prev.stdout, stdout=subprocess.PIPE, stderr=stderr_to, env=env)
        prev.stdout.close()  # Allow p1 to receive a SIGPIPE if p2 exits.
        prev = p

    p = subprocess.Popen(shlex.split(cmdlist[-1]), stdin=prev.stdout, stdout=stdout_to, stderr=stderr_to, env=env)

    output, err = p.communicate()
    # todo check error - also check that the checking deos not break the existing usage
    # we are actually checking if this is a file, subprocess.PIPE adn subprocess.DEVNULL are ints
    if hasattr(stdout_to, 'write'): stdout_to.close()
    if hasattr(stderr_to, 'write') and stdout_to != stderr_to : stderr_to.close()

    # p1.returncode will be None in this case (? is this always true)
    ok = p.returncode == 0
    return ok, (output.decode('utf-8') if (ok and output) else ""), err


def run_subprocess(cmd_string, env_variables=None, unset_env_vars=None, env_vars_extend=None,
                   noexit=False, stdoutfnm=None, errorfnm=None, logspecial=None, cwd=None):
    # we take a space-separated string as the input, but subprocess.run() likes
    # to have it as list, so we do split()
    # capture_output exists in  python  > 3.7, but let's jut keep this piece of code now that we have it

    # in a couple of places in subprocess.py we see the snippet
    # if env is None:
    #    env = os.environ
    # so if we pass None as env it will not obliterate the os.environ, but use it
    env = set_env(env_variables, unset_env_vars, env_vars_extend)
    # from https://docs.python.org/3/library/subprocess.html#security-considerations
    # Unlike some other popen functions, this implementation will never implicitly call a system shell.
    # This means that all characters, including shell metacharacters, can safely be passed to child processes.
    # If the shell is invoked explicitly, via shell=True, it is the application’s responsibility to ensure that all
    # whitespace and metacharacters are quoted appropriately to avoid shell injection vulnerabilities.
    # If shell is False, the first argument to run must be a list, e.g. ["ls", "-l", "/dev/null"]
    # (careful if ever attempting to set shell=True here - the argument with spaces would have to be quoted)
    stdout_to = open(stdoutfnm, "a+") if stdoutfnm else subprocess.PIPE
    stderr_to = open(errorfnm, "a+") if errorfnm else subprocess.PIPE

    # if  capture_output=False stderr is not captured (and neither is stdout)
    ret = subprocess.run(shlex.split(cmd_string), stdout=stdout_to, stderr=stderr_to, env=env, cwd=cwd)

    # we are actually checking if this is a file, subprocess.PIPE adn subprocess.DEVNULL are ints
    if hasattr(stdout_to, 'write'): stdout_to.close()
    if hasattr(stderr_to, 'write') and stdout_to != stderr_to: stderr_to.close()

    try:
        ret.check_returncode()  # if the return code is non-zero, raises a CalledProcessError.
    except subprocess.CalledProcessError as e:
        errmsg = f"\nin {os.getcwd()}\nwhile running {cmd_string}\n{e}\n"
        if ret.stderr: errmsg += ret.stderr.decode('utf-8') + "\n"
        if not logspecial:
            logger.error(errmsg)
        elif type(logspecial) == logging.Logger:
            logspecial.error(errmsg)
        elif logspecial == sys.stdout or logspecial == sys.stdin:
            print(errmsg, file=logspecial)
        else:
            print(errmsg, file=sys.stderr)

        if noexit:
            return False
        else:
            exit(1)

    return ret.stdout.decode('utf-8').strip() if ret.stdout else None

