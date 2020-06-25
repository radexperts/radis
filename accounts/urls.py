from django.contrib.auth import views as auth_views
from django.urls import path
from . import views

urlpatterns = [
    path('login/', views.LoginViewWithSuccessMsg.as_view(), name='login'),
    path('logout/', auth_views.LogoutView.as_view(
        template_name='accounts/logged_out.html'
    ), name='logout'),
    path('<int:pk>/', views.UserProfileView.as_view(), name='user_profile'),
    path('register/', views.RegistrationView.as_view(), name='registration'),
]