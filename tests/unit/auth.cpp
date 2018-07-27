#include <iostream>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "auth/auth.hpp"
#include "auth/crypto.hpp"
#include "utils/cast.hpp"
#include "utils/file.hpp"

using namespace auth;
namespace fs = std::experimental::filesystem;

class AuthWithStorage : public ::testing::Test {
 protected:
  virtual void SetUp() { utils::EnsureDir(test_folder_); }

  virtual void TearDown() { fs::remove_all(test_folder_); }

  fs::path test_folder_{
      fs::temp_directory_path() /
      ("unit_auth_test_" + std::to_string(static_cast<int>(getpid())))};

  Auth auth{test_folder_};
};

TEST_F(AuthWithStorage, AddRole) {
  ASSERT_TRUE(auth.AddRole("admin"));
  ASSERT_TRUE(auth.AddRole("user"));
  ASSERT_FALSE(auth.AddRole("admin"));
}

TEST_F(AuthWithStorage, RemoveRole) {
  ASSERT_TRUE(auth.AddRole("admin"));
  ASSERT_TRUE(auth.RemoveRole("admin"));
  ASSERT_FALSE(auth.RemoveRole("user"));
  ASSERT_FALSE(auth.RemoveRole("user"));
}

TEST_F(AuthWithStorage, AddUser) {
  ASSERT_FALSE(auth.HasUsers());
  ASSERT_TRUE(auth.AddUser("test"));
  ASSERT_TRUE(auth.HasUsers());
  ASSERT_TRUE(auth.AddUser("test2"));
  ASSERT_FALSE(auth.AddUser("test"));
}

TEST_F(AuthWithStorage, RemoveUser) {
  ASSERT_FALSE(auth.HasUsers());
  ASSERT_TRUE(auth.AddUser("test"));
  ASSERT_TRUE(auth.HasUsers());
  ASSERT_TRUE(auth.RemoveUser("test"));
  ASSERT_FALSE(auth.HasUsers());
  ASSERT_FALSE(auth.RemoveUser("test2"));
  ASSERT_FALSE(auth.RemoveUser("test"));
  ASSERT_FALSE(auth.HasUsers());
}

TEST_F(AuthWithStorage, Authenticate) {
  ASSERT_FALSE(auth.HasUsers());

  auto user = auth.AddUser("test");
  ASSERT_NE(user, std::experimental::nullopt);
  ASSERT_TRUE(auth.HasUsers());

  ASSERT_THROW(auth.Authenticate("test", "123"), utils::BasicException);

  user->UpdatePassword("123");
  ASSERT_TRUE(auth.SaveUser(*user));

  ASSERT_NE(auth.Authenticate("test", "123"), std::experimental::nullopt);

  ASSERT_EQ(auth.Authenticate("test", "456"), std::experimental::nullopt);
  ASSERT_NE(auth.Authenticate("test", "123"), std::experimental::nullopt);
}

TEST_F(AuthWithStorage, UserRolePermissions) {
  ASSERT_FALSE(auth.HasUsers());
  ASSERT_TRUE(auth.AddUser("test"));
  ASSERT_TRUE(auth.HasUsers());

  auto user = auth.GetUser("test");
  ASSERT_NE(user, std::experimental::nullopt);

  // Test initial user permissions.
  ASSERT_EQ(user->permissions().Has(Permission::Read),
            PermissionLevel::Neutral);
  ASSERT_EQ(user->permissions().Has(Permission::Create),
            PermissionLevel::Neutral);
  ASSERT_EQ(user->permissions().Has(Permission::Update),
            PermissionLevel::Neutral);
  ASSERT_EQ(user->permissions().Has(Permission::Delete),
            PermissionLevel::Neutral);
  ASSERT_EQ(user->permissions(), user->GetPermissions());

  // Change one user permission.
  user->permissions().Grant(Permission::Read);

  // Check permissions.
  ASSERT_EQ(user->permissions().Has(Permission::Read), PermissionLevel::Grant);
  ASSERT_EQ(user->permissions().Has(Permission::Create),
            PermissionLevel::Neutral);
  ASSERT_EQ(user->permissions().Has(Permission::Update),
            PermissionLevel::Neutral);
  ASSERT_EQ(user->permissions().Has(Permission::Delete),
            PermissionLevel::Neutral);
  ASSERT_EQ(user->permissions(), user->GetPermissions());

  // Create role.
  ASSERT_TRUE(auth.AddRole("admin"));
  auto role = auth.GetRole("admin");
  ASSERT_NE(role, std::experimental::nullopt);

  // Assign permissions to role and role to user.
  role->permissions().Grant(Permission::Delete);
  user->SetRole(*role);

  // Check permissions.
  {
    auto permissions = user->GetPermissions();
    ASSERT_EQ(permissions.Has(Permission::Read), PermissionLevel::Grant);
    ASSERT_EQ(permissions.Has(Permission::Delete), PermissionLevel::Grant);
    ASSERT_EQ(permissions.Has(Permission::Create), PermissionLevel::Neutral);
    ASSERT_EQ(permissions.Has(Permission::Update), PermissionLevel::Neutral);
  }

  // Add explicit deny to role.
  role->permissions().Deny(Permission::Read);
  user->SetRole(*role);

  // Check permissions.
  {
    auto permissions = user->GetPermissions();
    ASSERT_EQ(permissions.Has(Permission::Read), PermissionLevel::Deny);
    ASSERT_EQ(permissions.Has(Permission::Delete), PermissionLevel::Grant);
    ASSERT_EQ(permissions.Has(Permission::Create), PermissionLevel::Neutral);
    ASSERT_EQ(permissions.Has(Permission::Update), PermissionLevel::Neutral);
  }
}

TEST_F(AuthWithStorage, RoleManipulations) {
  {
    auto user1 = auth.AddUser("user1");
    ASSERT_TRUE(user1);
    auto role1 = auth.AddRole("role1");
    ASSERT_TRUE(role1);
    user1->SetRole(*role1);
    auth.SaveUser(*user1);

    auto user2 = auth.AddUser("user2");
    ASSERT_TRUE(user2);
    auto role2 = auth.AddRole("role2");
    ASSERT_TRUE(role2);
    user2->SetRole(*role2);
    auth.SaveUser(*user2);
  }

  {
    auto user1 = auth.GetUser("user1");
    ASSERT_TRUE(user1);
    auto role1 = user1->role();
    ASSERT_TRUE(role1);
    ASSERT_EQ(role1->rolename(), "role1");

    auto user2 = auth.GetUser("user2");
    ASSERT_TRUE(user2);
    auto role2 = user2->role();
    ASSERT_TRUE(role2);
    ASSERT_EQ(role2->rolename(), "role2");
  }

  ASSERT_TRUE(auth.RemoveRole("role1"));

  {
    auto user1 = auth.GetUser("user1");
    ASSERT_TRUE(user1);
    auto role = user1->role();
    ASSERT_FALSE(role);

    auto user2 = auth.GetUser("user2");
    ASSERT_TRUE(user2);
    auto role2 = user2->role();
    ASSERT_TRUE(role2);
    ASSERT_EQ(role2->rolename(), "role2");
  }

  {
    auto role1 = auth.AddRole("role1");
    ASSERT_TRUE(role1);
  }

  {
    auto user1 = auth.GetUser("user1");
    ASSERT_TRUE(user1);
    auto role1 = user1->role();
    ASSERT_FALSE(role1);

    auto user2 = auth.GetUser("user2");
    ASSERT_TRUE(user2);
    auto role2 = user2->role();
    ASSERT_TRUE(role2);
    ASSERT_EQ(role2->rolename(), "role2");
  }
}

TEST(AuthWithoutStorage, Permissions) {
  Permissions permissions;
  ASSERT_EQ(permissions.grants(), 0);
  ASSERT_EQ(permissions.denies(), 0);

  permissions.Grant(Permission::Read);
  ASSERT_EQ(permissions.Has(Permission::Read), PermissionLevel::Grant);
  ASSERT_EQ(permissions.grants(), utils::UnderlyingCast(Permission::Read));
  ASSERT_EQ(permissions.denies(), 0);

  permissions.Revoke(Permission::Read);
  ASSERT_EQ(permissions.Has(Permission::Read), PermissionLevel::Neutral);
  ASSERT_EQ(permissions.grants(), 0);
  ASSERT_EQ(permissions.denies(), 0);

  permissions.Deny(Permission::Read);
  ASSERT_EQ(permissions.Has(Permission::Read), PermissionLevel::Deny);
  ASSERT_EQ(permissions.denies(), utils::UnderlyingCast(Permission::Read));
  ASSERT_EQ(permissions.grants(), 0);

  permissions.Grant(Permission::Read);
  ASSERT_EQ(permissions.Has(Permission::Read), PermissionLevel::Grant);
  ASSERT_EQ(permissions.grants(), utils::UnderlyingCast(Permission::Read));
  ASSERT_EQ(permissions.denies(), 0);

  permissions.Deny(Permission::Create);
  ASSERT_EQ(permissions.Has(Permission::Read), PermissionLevel::Grant);
  ASSERT_EQ(permissions.Has(Permission::Create), PermissionLevel::Deny);
  ASSERT_EQ(permissions.Has(Permission::Update), PermissionLevel::Neutral);
  ASSERT_EQ(permissions.grants(), utils::UnderlyingCast(Permission::Read));
  ASSERT_EQ(permissions.denies(), utils::UnderlyingCast(Permission::Create));

  permissions.Grant(Permission::Delete);
  ASSERT_EQ(permissions.Has(Permission::Read), PermissionLevel::Grant);
  ASSERT_EQ(permissions.Has(Permission::Create), PermissionLevel::Deny);
  ASSERT_EQ(permissions.Has(Permission::Update), PermissionLevel::Neutral);
  ASSERT_EQ(permissions.Has(Permission::Delete), PermissionLevel::Grant);
  ASSERT_EQ(permissions.grants(),
            utils::UnderlyingCast(Permission::Read) |
                utils::UnderlyingCast(Permission::Delete));
  ASSERT_EQ(permissions.denies(), utils::UnderlyingCast(Permission::Create));

  permissions.Revoke(Permission::Delete);
  ASSERT_EQ(permissions.Has(Permission::Read), PermissionLevel::Grant);
  ASSERT_EQ(permissions.Has(Permission::Create), PermissionLevel::Deny);
  ASSERT_EQ(permissions.Has(Permission::Update), PermissionLevel::Neutral);
  ASSERT_EQ(permissions.Has(Permission::Delete), PermissionLevel::Neutral);
  ASSERT_EQ(permissions.grants(), utils::UnderlyingCast(Permission::Read));
  ASSERT_EQ(permissions.denies(), utils::UnderlyingCast(Permission::Create));
}

TEST(AuthWithoutStorage, PermissionsMaskTest) {
  Permissions p1(0, 0);
  ASSERT_EQ(p1.grants(), 0);
  ASSERT_EQ(p1.denies(), 0);

  Permissions p2(1, 0);
  ASSERT_EQ(p2.grants(), 1);
  ASSERT_EQ(p2.denies(), 0);

  Permissions p3(1, 1);
  ASSERT_EQ(p3.grants(), 0);
  ASSERT_EQ(p3.denies(), 1);

  Permissions p4(3, 2);
  ASSERT_EQ(p4.grants(), 1);
  ASSERT_EQ(p4.denies(), 2);
}

TEST(AuthWithoutStorage, UserSerializeDeserialize) {
  auto user = User("test");
  user.permissions().Grant(Permission::Read);
  user.permissions().Deny(Permission::Update);
  user.UpdatePassword("world");

  auto data = user.Serialize();

  auto output = User::Deserialize(data);
  ASSERT_EQ(user, output);
}

TEST(AuthWithoutStorage, RoleSerializeDeserialize) {
  auto role = Role("test");
  role.permissions().Grant(Permission::Read);
  role.permissions().Deny(Permission::Update);

  auto data = role.Serialize();

  auto output = Role::Deserialize(data);
  ASSERT_EQ(role, output);
}

TEST_F(AuthWithStorage, UserWithRoleSerializeDeserialize) {
  auto role = auth.AddRole("test");
  ASSERT_TRUE(role);
  role->permissions().Grant(Permission::Read);
  role->permissions().Deny(Permission::Update);
  auth.SaveRole(*role);

  auto user = auth.AddUser("test");
  ASSERT_TRUE(user);
  user->permissions().Grant(Permission::Read);
  user->permissions().Deny(Permission::Update);
  user->UpdatePassword("world");
  user->SetRole(*role);
  auth.SaveUser(*user);

  auto new_user = auth.GetUser("test");
  ASSERT_TRUE(new_user);
  ASSERT_EQ(*user, *new_user);
}

TEST(AuthWithoutStorage, Crypto) {
  auto hash = EncryptPassword("hello");
  ASSERT_TRUE(VerifyPassword("hello", hash));
  ASSERT_FALSE(VerifyPassword("hello1", hash));
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  google::InitGoogleLogging(argv[0]);
  return RUN_ALL_TESTS();
}
