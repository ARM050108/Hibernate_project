package jm.task.core.jdbc;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.service.UserService;
import jm.task.core.jdbc.service.UserServiceImpl;

import java.util.List;

public class Main {
    public static void main(String[] args) {
        UserService userService = new UserServiceImpl();

        userService.createUsersTable();

        userService.saveUser("John", "Doe", (byte) 25);
        userService.saveUser("Jane", "Smith", (byte) 30);
        userService.saveUser("Michael", "Johnson", (byte) 22);
        userService.saveUser("Emily", "Davis", (byte) 27);

        List<User> users = userService.getAllUsers();
        for (User user : users) {
            System.out.println(user);
        }

        userService.cleanUsersTable();
        userService.dropUsersTable();
    }
}
