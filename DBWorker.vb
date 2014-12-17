Imports System.Data.SqlClient
Imports System.Data.Odbc
Imports System.Data

Public Class DBWorker

    Public lTrans As Boolean = False
    Public oConnection As System.Data.IDbConnection
    Private oAdapter As System.Data.IDbDataAdapter
    Private oCommand As System.Data.IDbCommand

    Public Enum DB
        SQL = 1
        ODBC = 2
    End Enum
    Public eDB As DB

    Public ReadOnly Property ConnState() As ConnectionState
        Get
            Return oConnection.State
        End Get
    End Property

    Private cODBCDSN, cODBCUser, cODBCPass As String
    Private cDBServer, cDBDataBase, cDBUser, cDBPass As String
    Private lDBSQLUser As Boolean

    Friend Sub New(ByVal nDB As DB)
        Select Case nDB
            Case DB.SQL
                eDB = DB.SQL
                oCommand = New SqlCommand()
                oConnection = New SqlConnection()
                oAdapter = New SqlDataAdapter()
            Case DB.ODBC
                eDB = DB.ODBC
                oCommand = New OdbcCommand
                oConnection = New OdbcConnection()
                oAdapter = New OdbcDataAdapter()
        End Select
    End Sub

    Friend Overloads Sub ConnectDB(ByVal cDSN As String, ByVal cUser As String, ByVal cPass As String)
        '22/11/10 - Ahora guardamos las variables para reconectar ante un error
        cODBCDSN = cDSN
        cODBCUser = cUser
        cODBCPass = cPass
        Dim cConnString As String = "DSN=" & cDSN & ";UID=" & cUser & ";PWD=" & cPass & ";"
        'Me.ConnectODBC(cDSN, cUser, cPass, stError)
        oConnection = New OdbcConnection(cConnString)
        oCommand.Connection = oConnection
    End Sub

    Friend Overloads Sub ConnectDB(ByVal cServer As String, ByVal cDataBase As String, _
            ByVal lSQLUser As Boolean, ByVal cUser As String, ByVal cPass As String)

        '22/11/10 - Ahora guardamos las variables para reconectar ante un error
        cDBServer = cServer
        cDBDataBase = cDataBase
        cDBUser = cUser
        cDBPass = cPass
        Dim cConnString As String
        cConnString = "Persist Security Info=False;Integrated Security=" + CStr(Not lSQLUser) + ";" & _
            "user id=" + cUser + ";pwd=" + cPass + ";database=" & cDataBase & ";server=" & cServer
        'Me.ConnectSQL(cServer, cDataBase, lSQLUser, cUser, cPass, stError)
        oConnection = New SqlConnection(cConnString)
        oCommand.Connection = oConnection
    End Sub

    Friend Function GetDataTable(ByVal cSQL As String, Optional ByVal cTablename As String = "SQLRESULT", Optional cDateformat As String = "mdy") As DataTable
        Dim dsData As New DataSet()
        Try
            openConnection()
            oCommand.CommandText = "set dateformat " + cDateformat
            oCommand.ExecuteNonQuery()

            oCommand.CommandText = cSQL


            oAdapter.SelectCommand = oCommand
            oAdapter.Fill(dsData)

            closeConnection()
            '
            ' Comprobamos que la consulta se ha ejecutado correctamente.
            If dsData.Tables.Count = 0 Then
                Return Nothing
            Else
                Return dsData.Tables(0)
            End If
        Catch ex As Exception
            WriteLogDevice(ex)
        End Try
        Return Nothing
    End Function

    Friend Function GetDataRow(ByVal cSQL As String, Optional cDateformat As String = "mdy") As DataRow
        Dim dsData As New DataSet()
        Try
            openConnection()
            oCommand.CommandText = "set dateformat " + cDateformat
            oCommand.ExecuteNonQuery()

            oCommand.CommandText = cSQL

            oAdapter.SelectCommand = oCommand
            oAdapter.Fill(dsData)

            closeConnection()
            '
            ' Comprobamos que la consulta se ha ejecutado correctamente.
            If dsData.Tables.Count = 0 Then
                Return Nothing
            ElseIf dsData.Tables(0).Rows.Count = 0 Then
                Return Nothing
            ElseIf dsData.Tables(0).Rows.Count > 1 Then
                Return Nothing
            Else
                Return dsData.Tables(0).Rows(0)
            End If
        Catch ex As Exception
            WriteLogDevice(ex)
        End Try
        Return Nothing
    End Function

    Public Function GetDataItem(Of T)(ByVal cSQL As String, Optional cDateformat As String = "mdy") As T
        Dim dsData As New DataSet()
        Try
            openConnection()
            oCommand.CommandText = "set dateformat " + cDateformat
            oCommand.ExecuteNonQuery()

            oCommand.CommandText = cSQL
            oAdapter.SelectCommand = oCommand
            oAdapter.Fill(dsData)

            closeConnection()
            '
            ' Comprobamos que la consulta se ha ejecutado correctamente.
            If dsData.Tables.Count = 0 Then
                Return getTypeDefault(GetType(T))
            ElseIf dsData.Tables(0).Rows.Count = 0 Then
                Return getTypeDefault(GetType(T))
            ElseIf dsData.Tables(0).Rows.Count > 1 Then
                Return getTypeDefault(GetType(T))
            ElseIf dsData.Tables(0).Columns.Count > 1 Then
                Return getTypeDefault(GetType(T))
            Else
                Return dsData.Tables(0).Rows(0).Item(0)
            End If
        Catch ex As Exception
            WriteLogDevice(ex)
        End Try

        Return getTypeDefault(GetType(T))
    End Function

    Friend Function GetID() As Integer
        Dim dsData As New DataSet
        Try
            oCommand.CommandText = "Select id from Contador"
            openConnection()

            oAdapter.SelectCommand = oCommand
            oAdapter.Fill(dsData)
            oCommand.CommandText = "UPDATE contador set id = id+1"
            oCommand.ExecuteNonQuery()

            closeConnection()
        Catch ex As Exception
            WriteLogDevice(ex)
        End Try
        '
        ' Comprobamos que la consulta se ha ejecutado correctamente.
        If dsData.Tables.Count = 0 Then
            Return Nothing
        ElseIf dsData.Tables(0).Rows.Count = 0 Then
            Return Nothing
        ElseIf dsData.Tables(0).Rows.Count > 1 Then
            Return Nothing
        ElseIf dsData.Tables(0).Columns.Count > 1 Then
            Return Nothing
        Else
            Return dsData.Tables(0).Rows(0).Item(0)
        End If
    End Function

    Friend Sub SQLExec(ByVal cSQL As String, Optional cDateformat As String = "mdy")

        Try
            openConnection()

            oCommand.CommandText = "set dateformat " + cDateformat
            oCommand.ExecuteNonQuery()

            oCommand.CommandText = cSQL
            oCommand.ExecuteNonQuery()

            closeConnection()
        Catch ex As Exception
            WriteLogDevice(ex.Message & " - " & cSQL)
        End Try

    End Sub

    Friend Sub InitTrans()
        Try
            If Not lTrans Then
                openConnection()
                '
                oCommand.Transaction = oConnection.BeginTransaction()
            End If

            lTrans = True
        Catch ex As Exception
            WriteLogDevice(ex)
        End Try
    End Sub

    Friend Sub CommitTrans()
        Try
            If lTrans Then
                oCommand.Transaction.Commit()
                '
                closeConnection()
            End If

            lTrans = False
        Catch ex As Exception
            WriteLogDevice(ex)
        End Try
    End Sub

    Friend Sub RollbackTrans()
        If lTrans Then
            'No se puede hacer rollback si no hay transaccion abierta

            Try
                oCommand.Transaction.Rollback()
                '
                closeConnection()
            Catch ex As Exception
                WriteLogDevice(ex)
            End Try

            oCommand.Transaction = Nothing
            lTrans = False
        End If

    End Sub

    Private Sub openConnection()
        If ConnState() = ConnectionState.Closed Then
            oConnection.Open()
        End If
    End Sub

    Private Sub closeConnection()
        If oCommand.Transaction Is Nothing Then
            If ConnState() = ConnectionState.Open Then
                oConnection.Close()
            End If
        End If
    End Sub

    Friend Sub Dispose()
        If oConnection.State = ConnectionState.Open Then
            oConnection.Close()
        End If
        oConnection.Dispose()
    End Sub
End Class
