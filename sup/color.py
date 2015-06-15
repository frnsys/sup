from colorama import Fore, Back, Style


def cprint(label, subject, color=Fore.GREEN):
    print('{}{}{}'.format(color, label, Fore.RESET),
          subject)
