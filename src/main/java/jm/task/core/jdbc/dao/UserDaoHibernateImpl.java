package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class UserDaoHibernateImpl implements UserDao {
    private static final Logger logger = Logger.getLogger(UserDaoHibernateImpl.class.getName());
    private final SessionFactory sessionFactory = Util.getSessionFactory(); // создали поле sessionFactory

    public UserDaoHibernateImpl() {
    }

    private void executeTransaction(TransactionAction action) {
        Transaction transaction = null;
        try (Session session = sessionFactory.openSession()) { // используем поле sessionFactory
            transaction = session.beginTransaction();
            action.execute(session);
            transaction.commit();
        } catch (Exception e) {
            if (transaction != null) transaction.rollback();
            logger.log(Level.SEVERE, "Transaction failed", e);
        }
    }

    @Override
    public void createUsersTable() {
        String sql = "CREATE TABLE IF NOT EXISTS users " +
                "(id BIGINT NOT NULL AUTO_INCREMENT, " +
                "name VARCHAR(50), " +
                "lastName VARCHAR(50), " +
                "age TINYINT, " +
                "PRIMARY KEY (id))";

        executeTransaction(session -> {
            session.createSQLQuery(sql).executeUpdate();
        });
    }

    @Override
    public void dropUsersTable() {
        String sql = "DROP TABLE IF EXISTS users";

        executeTransaction(session -> {
            session.createSQLQuery(sql).executeUpdate();
        });
    }

    @Override
    public void saveUser(String name, String lastName, byte age) {
        User user = new User(name, lastName, age);

        executeTransaction(session -> {
            session.save(user);
        });
    }

    @Override
    public void removeUserById(long id) {
        executeTransaction(session -> {
            User user = session.get(User.class, id);
            if (user != null) {
                session.delete(user);
            }
        });
    }

    @Override
    public List<User> getAllUsers() {
        List<User> users = null;
        try (Session session = sessionFactory.openSession()) { // используем поле sessionFactory
            users = session.createQuery("FROM User", User.class).list();
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to get all users", e);
        }
        return users;
    }

    @Override
    public void cleanUsersTable() {
        String sql = "DELETE FROM users";

        executeTransaction(session -> {
            session.createSQLQuery(sql).executeUpdate();
        });
    }

    @FunctionalInterface
    private interface TransactionAction {
        void execute(Session session);
    }
}

