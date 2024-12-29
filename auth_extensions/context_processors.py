
def anon_aware_authentication(request):
    has_permission = request.user.is_authenticated and request.user.username != 'anon'
    return {
        'is_authenticated_and_not_anon': has_permission,
    }