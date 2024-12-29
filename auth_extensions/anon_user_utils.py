from topics.models import IndustryCluster
from random import randrange, shuffle
from django.core.cache import cache
from rest_framework.permissions import IsAuthenticated
from django.contrib.auth import get_user_model

PASSWORD_CACHE_KEY="anon_password"
ANON_USERNAME="anon"

def generate_password():
    ind = IndustryCluster.nodes.filter(representation__isnull=False).order_by('?')[0]
    word_len = len(ind.representation)
    start = randrange(0, (word_len-2))
    words = [x for x in ind.representation if len(x) > 0]
    shuffle(words) 
    return "+".join( words[start:start+2] )

def create_anon_user():
    password = generate_password()
    user, _ = get_user_model().objects.get_or_create(username=ANON_USERNAME)
    user.set_password(password)
    user.is_active = True
    user.save()
    cache.set(PASSWORD_CACHE_KEY, password)
    return user, password

def get_anon_password():
    pwd = cache.get(PASSWORD_CACHE_KEY)
    if pwd is None:
        _, pwd = create_anon_user()
    return pwd

class IsAuthenticatedNotAnon(IsAuthenticated):

    def has_permission(self, request, view):
        is_authenticated = super().has_permission(request, view)
        if not is_authenticated:
            return False
        
        is_anon = True if request.user.username == ANON_USERNAME else False
        if is_anon:
            return False
        
        return True

