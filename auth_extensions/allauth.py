# https://docs.allauth.org/en/dev/account/forms.html

from allauth.account.forms import LoginForm
from .anon_user_utils import get_anon_password

class AnonAwareLoginForm(LoginForm):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.anon_password = get_anon_password()
