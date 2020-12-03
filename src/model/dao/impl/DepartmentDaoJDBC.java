package model.dao.impl;

import db.DbException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import model.dao.DepartmentDao;
import model.entities.Department;
import db.DB;
import db.DbIntegrityException;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;

public class DepartmentDaoJDBC implements DepartmentDao{
    
    private Connection conn;
    
    public DepartmentDaoJDBC(Connection conn){
        this.conn = conn;
    }

    @Override
    public void insert(Department obj) {
        PreparedStatement st = null;
        ResultSet rs = null;
        try {
            st = conn.prepareStatement("INSERT INTO department(name) "
                    + "values(?)", Statement.RETURN_GENERATED_KEYS);
            st.setString(1, obj.getName());
            int rowsAffected = st.executeUpdate();
            if (rowsAffected > 0){
                rs = st.getGeneratedKeys();
                if (rs.next()){
                    obj.setId(rs.getInt(1));
                }
            } else {
                throw new DbException("Unexpected error! No rows were affected!");
            }
            
        } catch(SQLException e){
            throw new DbException(e.getMessage());
        } finally{
            DB.closeResultSet(rs);
            DB.closeStatement(st);
        }
    }

    @Override
    public void update(Department obj) {
        PreparedStatement st = null;
        try {
            st = conn.prepareStatement("UPDATE department set "
                    + "name = ? "
                    + "where id = ?");
            st.setString(1, obj.getName());
            st.setInt(2, obj.getId());
            st.executeUpdate();

        } catch (SQLException e){
            throw new DbException(e.getMessage());
        } finally {
            DB.closeStatement(st);
        }
    }

    @Override
    public void deleteById(Integer id) {
        PreparedStatement st = null;
        try{
            st = conn.prepareStatement("DELETE from department where id = ?");
            st.setInt(1, id);
            st.executeUpdate();

        } catch(SQLException e){
            throw new DbIntegrityException(e.getMessage());
        } finally {
            DB.closeStatement(st);
        }
    }

    @Override
    public Department findById(Integer id) {
        PreparedStatement st = null;
        ResultSet rs = null;
        try{
            st = conn.prepareStatement("select d.* "
                    + "from department d "
                    + "where d.id = ?");
            st.setInt(1, id);
            rs = st.executeQuery();
            
            if(rs.next()){
                Department dep = new Department(rs.getInt("Id"), rs.getString("Name"));
                return dep;
            }
            return null;
            
        } catch(SQLException e){
            throw new DbException(e.getMessage());
        } finally {
            DB.closeResultSet(rs);
            DB.closeStatement(st);
        }
    }

    @Override
    public List<Department> findAll() {
        PreparedStatement st = null;
        ResultSet rs = null;
        try{
            st = conn.prepareStatement("select * from department");
            rs = st.executeQuery();
            List<Department> departments = new ArrayList<>();
            
            while (rs.next()){
                Department dep = new Department(rs.getInt("Id"), rs.getString("Name"));
                departments.add(dep);
            }
            return departments;
            
        } catch(SQLException e){
            throw new DbException(e.getMessage());
        } finally {
            DB.closeResultSet(rs);
            DB.closeStatement(st);
        }
    }
}
