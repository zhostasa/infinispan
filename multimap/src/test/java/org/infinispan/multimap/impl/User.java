package org.infinispan.multimap.impl;

import static java.util.Objects.hash;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;

import org.infinispan.commons.marshall.Externalizer;
import org.infinispan.commons.marshall.SerializeWith;

@SerializeWith(User.UserExternalizer.class)
public class User implements Serializable {
   private final String name;
   private final int age;

   public User(String name, int age) {
      this.name = name;
      this.age = age;
   }

   public String getName() {
      return name;
   }

   public int getAge() {
      return age;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      User user = (User) o;

      if (age != user.age) return false;
      return name != null ? name.equals(user.name) : user.name == null;
   }

   @Override
   public int hashCode() {
      return hash(name, age);
   }

   @Override
   public String toString() {
      return "User{" +
            "name='" + name + '\'' +
            ", age=" + age +
            '}';
   }

   public static class UserExternalizer implements Externalizer<User> {

      @Override
      public void writeObject(ObjectOutput output, User object) throws IOException {
         output.writeObject(object.name);
         output.writeInt(object.age);
      }

      @Override
      public User readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         String name = (String) input.readObject();
         int age = input.readInt();
         return new User(name, age);
      }
   }
}
