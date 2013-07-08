/* Copyright (c) 2001-2011, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


package org.hsqldb.jdbc.testbase;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.hsqldb.resources.ResourceBundleHandler;

/**
 * Encapsulates an SQLSTATE parameter value, as described in
 * 6WD2-02-Foundation-2007-12 (ISO/IEC 9075-2).<p>
 *
 * The character string value returned in an SQLSTATE parameter comprises a
 * 2-character class value followed by a 3-character subclass value, each
 * with an implementation-defined character set that has a one-octet character
 * encoding form and is restricted to *lt;digit&gt;s and &lt;simple Latin upper
 * case letter&gt;s. 5CD2-02-Foundation-2006-01, Subclause 24.1, "SQLSTATE",
 * Table 33, "SQLSTATE class and subclass values", specifies the class value for
 * each condition and the subclass value or values for each class value.<p>
 *
 * Table 33 is modified by:<p>
 *
 * <ul>
 * <li>Table 3, "SQLSTATE class and subclass values" in Subclause 20.1,
 *     "SQLSTATE", of 6WD2-04-PSM-2007-12 (ISO/IEC 9075-4).<p>
 * </li>
 * <li>Table 37, "SQLSTATE class and subclass values" in Subclause 26.1,
 *     "SQLSTATE", of 6WD2-09-MED-2007-12 (ISO/IEC 9075-09).<p>
 * </li>
 * <li>Table 21, "SQLSTATE class and subclass values" in Subclause 15.1,
 *     "SQLSTATE", of 6WD2-10-MED-2007-12 (ISO/IEC 9075-10).<p>
 * </li>
 * <li>Table 2, "SQLSTATE class and subclass values" in Subclause 15.1,
 *     "SQLSTATE", of 6WD2-13-JRT-2007-12 (ISO/IEC 9075-13).<p>
 * </li>
 * <li>Table 14, "SQLSTATE class and subclass values" in Subclause 23.1,
 *     "SQLSTATE", of 6WD2-14-XML-2007-12 (ISO/IEC 9075-14).
 * </li>
 * </ul><p>
 *
 * Class values that begin with one of the &lt;digit&gt;s '0', '1', '2', '3',
 * or '4' or one of the &lt;simple Latin upper case letter*gt;s 'A', 'B', 'C',
 * 'D', 'E', 'F', 'G', or 'H' are returned only for conditions defined in
 * ISO/IEC 9075 or in any other International Standard. The range of such
 * class values are called <em>standard-defined classes</em>. Some such class
 * codes are reserved for use by specific International Standards, as specified
 * elsewhere. Subclass values associated with such classes that also begin
 * with one of those 13 characters are returned only for conditions defined in
 * ISO/IEC 9075 or some other International Standard. The range of such class
 * values are called <em>standard-defined classes</em>. Subclass values
 * associated with such classes that begin with one of the &lt;digit&gt;s '5',
 * '6', '7', '8', or '9' or one of the &lt;simple Latin upper case letter&gt;s
 * 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W',
 * 'X', 'Y', or 'Z' are reserved for implementation-specified conditions and are
 * called implementation-defined subclasses. <p>
 *
 * Class values that begin with one of the &lt;digit&gt;s '5', '6', '7', '8',
 * or '9' or one of the &lt;simple Latin upper case letter&gt;s 'I', 'J', 'K',
 * 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', or 'Z'
 * are reserved for implementation-specified exception conditions and are called
 * implementation-defined classes. All subclass values except '000', which means
 * no subclass, associated with such classes are reserved for
 * implementation-specified conditions and are called implementation-defined
 * subclasses. An implementation-defined completion condition shall be
 * indicated by returning an implementation-defined subclass in conjunction with
 * one of the classes: successful completion, warning, or no data. <p>
 *
 * If a subclass value is not specified for a condition, then either subclass
 * '000' or an implementation-defined subclass is returned. <p>
 *
 * <b>NOTE</b>: One consequence of this is that an SQL-implementation may, but
 * is not required by ISO/IEC 9075 to, provide subcodes for exception condition
 * syntax error or access rule violation that distinguish between the syntax
 * error and access rule violation cases. <p>
 *
 * If multiple completion conditions: warning or multiple exception conditions,
 * including implementation-defined exception conditions, are raised, then it
 * is implementation-dependent which of the corresponding SQLSTATE values is
 * returned in the SQLSTATE status parameter, provided that the precedence
 * rules in 6WD2-02-Foundation-2007-12, Subclause 4.29.2, "Status parameters",
 * are obeyed. Any number of applicable conditions values in addition to the
 * one returned in the SQLSTATE status parameter, may be returned in the
 * diagnostics area. <p>
 *
 * An implementation-specified condition may duplicate, in whole or in part,
 * a condition defined in ISO/IEC 9075; however, if such a condition occurs as
 * a result of executing a statement, then the corresponding implementation-
 * defined SQLSTATE value shall not be returned in the SQLSTATE parameter but
 * may be returned in the diagnostics area. <p>
 *
 * The {@link #Category} value has the following meanings:
 *
 * <ul>
 * <li>"S" denotes the {@link #Class} value corresponds to successful
 *     completion and is a completion condition;<p>
 * </li>
 * <li>"W" denotes the {@link #Class} value corresponds to a successful
 *     completion but with a warning and is a completion condition;<p>
 * </li>
 * <li>"N" denotes the {@link #Class} value corresponds to a no-data situation
 *     and is a completion condition;</p>
 * </li>
 * <li>"X" denotes the {@link #Class} value corresponds to an exception
 *     condition.
 * </li>
 * </ul>
 *
 * @author Campbell Boucher-Burnet &lt;boucherb at users.sourceforge.net&gt;
 */
public abstract class SqlState implements Serializable {

    private static final long serialVersionUID = 1L;
    /**
     * Has the following meanings:
     *
     * <ul>
     * <li>"S" denotes the {@link #Class} value corresponds to successful
     *     completion and is a completion condition;<p>
     * </li>
     * <li>"W" denotes the {@link #Class} value corresponds to a successful
     *     completion but with a warning and is a completion condition;<p>
     * </li>
     * <li>"N" denotes the {@link #Class} value corresponds to a no-data
     *     situation and is a completion condition;</p>
     * </li>
     * <li>"X" denotes the {@link #Class} value corresponds to an exception
     *     condition.</li>
     * </ul>
     */
    public transient final char Category;
    /**
     * As defined in the "Condition" column of 6WD2-02-Foundation-2007-12,
     * Subclause 24.1, "SQLSTATE", Table 33, "SQLSTATE class and subclass
     * values" or modifications thereof.<p>
     *
     * May be a localized version.
     */
    public transient final String Condition;
    /**
     * As defined in the "Class" column of 6WD2-02-Foundation-2007-12,
     * Subclause 24.1, "SQLSTATE", Table 33, "SQLSTATE class and subclass
     * values" or modifications thereof.<p>
     */
    public transient final String Class;
    /**
     * <tt>true</tt> if {@link #Class} begins with one of the
     * &lt;digit&gt;s '0', '1', '2', '3', or '4' or one of the
     * &lt;simple Latin upper case letter*gt;s 'A', 'B', 'C',
     * 'D', 'E', 'F', 'G', or 'H'.
     */
    public transient final boolean ClassIsStandardDefined;
    /**
     * <tt>true</tt> if {@link #Class} begins with one of the
     * &lt;digit&gt;s '5', '6', '7', '8', or '9' or one of the
     * &lt;simple Latin upper case letter&gt;s 'I', 'J', 'K',
     * 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V',
     * 'W', 'X', 'Y', or 'Z'.
     */
    public transient final boolean ClassIsImplemenationDefined;
    /**
     * As defined in the "Subcondition" column of 5CD2-02-Foundation-2006-01,
     * Subclause 24.1, "SQLSTATE", Table 33, "SQLSTATE class and subclass
     * values" or modifications thereof.<p>
     *
     * May be a localized version.
     */
    public transient final String Subcondition;
    /**
     * As defined in the "Subclass" column of 5CD2-02-Foundation-2006-01,
     * Subclause 24.1, "SQLSTATE", Table 33, "SQLSTATE class and subclass
     * values" or modifications thereof.<p>
     */
    public final String Subclass;
    /**
     * <tt>true</tt> if {@link #Class} begins with one of the
     * &lt;digit&gt;s '0', '1', '2', '3', or '4' or one of the
     * &lt;simple Latin upper case letter*gt;s 'A', 'B', 'C',
     * 'D', 'E', 'F', 'G', or 'H' and {@link #Subclass} begins
     * with one of the &lt;digit&gt;s '0', '1', '2', '3', or '4'
     * or one of the &lt;simple Latin upper case letter*gt;s 'A',
     * 'B', 'C', 'D', 'E', 'F', 'G', or 'H'
     */
    public transient final boolean SubclassIsStandardDefined;
    /**
     * <tt>true</tt> if {@link #Subclass} begins with one of the
     * &lt;digit&gt;s '5', '6', '7', '8', or '9' or one of the
     * &lt;simple Latin upper case letter&gt;s 'I', 'J', 'K',
     * 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V',
     * 'W', 'X', 'Y', or 'Z'
     */
    public transient final boolean SubclassIsImplementationDefined;
    /**
     * <tt>true</tt> if {@link #Subclass} equals
     * {@link Constant.SqlStateSubclass#NoSubclass}.
     */
    public transient final boolean IsNoSubclass;
    /**
     * The catenation of {@link #Class} and {@link #Subclass}.
     */
    public final String Value;
    /**
     * <tt>true</tt> if {@link #ClassIsStandardDefined} is <tt>true</tt> and
     * {@link #SubclassIsStandardDefined} is <tt>true</tt>.
     */
    public transient final boolean ValueDenotesStandardSpecifiedCondition;
    /**
     * <tt>true</tt> if {@link #ClassIsImplemenationDefined} is <tt>true</tt>
     * or {@link #ClassIsStandardDefined} is <tt>true</tt>  and
     * {@link #SubclassIsImplementationDefined} is <tt>true</tt>.
     */
    public transient final boolean ValueDenotesImplementationSpecifiedCondition;
    @SuppressWarnings("CollectionWithoutInitialCapacity")
    private static final Map<String, SqlState> s_map = new HashMap<String, SqlState>();
    public static final Map<String, SqlState> Map = Collections.unmodifiableMap(s_map);

    /**
     * Constructs a new {@link SqlState} from the given
     * <tt>sqlStateCategory</tt>, <tt>sqlStateClass</tt> and
     * <tt>sqlStateSubclass</tt>.<p>
     *
     * @param sqlStateClass of the SQLSTATE
     * @param sqlStateSubclass of the SQLSTATE
     * @throws IllegalArgumentException
     *         if <tt>sqlStateClass</tt> is null, its length is not 2,
     *         or it contains a character not in [1..9][A..Z]; if
     *         <tt>sqlStateSubclass</tt>
     */
    @SuppressWarnings("LeakingThisInConstructor")
    public SqlState(
            final String sqlStateClass,
            final String sqlStateSubclass) {

        SqlState.Routine.checkSqlStateClass(sqlStateClass);
        SqlState.Routine.checkSqlStateSubclass(sqlStateSubclass);

        //
        Condition = SqlState.Routine.conditionForSqlStateClass(sqlStateClass);
        //
        Class = sqlStateClass;
        ClassIsStandardDefined =
                SqlState.Routine.isStandardDefinedConditionSqlStateClass(sqlStateClass);
        ClassIsImplemenationDefined =
                SqlState.Routine.isImplementationDefinedConditionSqlStateClass(sqlStateClass);
        //
        Subclass = sqlStateSubclass;
        SubclassIsStandardDefined = ClassIsStandardDefined
                && SqlState.Routine.canBeStandardDefinedConditionSqlStateSubclass(sqlStateSubclass);
        SubclassIsImplementationDefined =
                SqlState.Routine.mustBeImplementationDefinedConditionSqlStateSubclass(sqlStateSubclass);
        IsNoSubclass = SqlState.Routine.isNoSubclass(sqlStateSubclass);
        //
        Value = sqlStateClass + sqlStateSubclass;
        ValueDenotesStandardSpecifiedCondition = ClassIsStandardDefined
                && SubclassIsStandardDefined;
        ValueDenotesImplementationSpecifiedCondition = ClassIsImplemenationDefined
                || (ClassIsStandardDefined && SubclassIsImplementationDefined);
        //
        Subcondition = SqlState.Routine.subConditionForSqlState(Value);
        //
        Category = SqlState.Routine.categoryForSqlState(Value);

        SqlState.s_map.put(Value, this);
    }

    @Override
    public boolean equals(Object obj) {
        return (obj instanceof SqlState)
                && Value.equals(((SqlState) obj).Value);
    }

    @Override
    public int hashCode() {
        return Value.hashCode();
    }

    @Override
    public String toString() {
        return Value + ' ' + Condition + ": " + Subcondition;
    }

    /**
     * Denotes an SQL Successful Completion Condition.
     */
    @SuppressWarnings("PublicInnerClass")
    public static final class SuccessfulCompletion extends SqlState {

        private static final long serialVersionUID = 1L;

        /**
         * Constructs a new instance with the given subclass.
         *
         * @param sqlStateSubclass of the new instance.
         */
        public SuccessfulCompletion(final String sqlStateSubclass) {
            super(Constant.SqlStateClass.SuccessfulCompletion,
                    sqlStateSubclass);
        }
        /**
         * Completion of the operation was successful and did not result in
         * any type of warning or exception condition.
         */
        public static final SuccessfulCompletion NoSubclass =
                new SuccessfulCompletion(Constant.SqlStateSubclass.NoSubclass);
    }

    /**
     *
     */
    @SuppressWarnings("PublicInnerClass")
    public static final class NoData extends SqlState {

        private static final long serialVersionUID = 1L;

        /**
         *
         * @param sqlStateSubclass
         */
        public NoData(final String sqlStateSubclass) {
            super(Constant.SqlStateClass.NoData,
                    sqlStateSubclass);
        }
        /**
         *
         */
        public static final NoData NoSubclass = new NoData(
                Constant.SqlStateSubclass.NoSubclass);
        /**
         *
         */
        public static final NoData NoAdditionalResultSetsReturned =
                new NoData(Constant.SqlStateSubclass.NoData.NoAdditionalResultSetsReturned);
    }

    /**
     *
     */
    @SuppressWarnings("PublicInnerClass")
    public static final class Warning extends SqlState {

        private static final long serialVersionUID = 1L;

        /**
         *
         * @param subClass
         */
        public Warning(final String subClass) {
            super(Constant.SqlStateClass.Warning, subClass);
        }
        /**
         *
         */
        public static final Warning NoSubclass =
                new Warning(Constant.SqlStateSubclass.NoSubclass);
        /**
         *
         */
        public static final Warning AdditionalResultSetsReturned = new Warning(
                Constant.SqlStateSubclass.Warning.AdditionalResultSetsReturned);
        /**
         *
         */
        public static final Warning ArrayDataRightTruncation = new Warning(
                Constant.SqlStateSubclass.Warning.ArrayDataRightTruncation);
        /**
         *
         */
        public static final Warning AttemptToReturnTooManyResultSets =
                new Warning(
                Constant.SqlStateSubclass.Warning.AttemptToReturnTooManyResultSets);
        /**
         *
         */
        public static final Warning CursorOperationConflict = new Warning(
                Constant.SqlStateSubclass.Warning.CursorOperationConflict);
        /**
         *
         */
        public static final Warning DefaultValueTooLongForInformationSchema =
                new Warning(
                Constant.SqlStateSubclass.Warning.DefaultValueTooLongForInformationSchema);
        /**
         * Denotes that a DISCONNECT error occurred.
         */
        public static final Warning DisconnectError = new Warning(
                Constant.SqlStateSubclass.Warning.DisconnectError);
        /**
         *
         */
        public static final Warning ResultSetsReturned = new Warning(
                Constant.SqlStateSubclass.Warning.ResultSetsReturned);
        /**
         * Insufficient number of entries in an SQLDA.
         */
        public static final Warning InsufficientItemDescriptorAreas =
                new Warning(
                Constant.SqlStateSubclass.Warning.InsufficientItemDescriptorAreas);
        /**
         * Denotes that Null values were eliminated from the argument of a column function.
         */
        public static final Warning NullValueEliminatedInSetFunction =
                new Warning(
                Constant.SqlStateSubclass.Warning.NullValueEliminatedInSetFunction);
        /**
         * A privilege was not granted.
         */
        public static final Warning PrivilegeNotGranted = new Warning(
                Constant.SqlStateSubclass.Warning.PrivilegeNotGranted);
        /**
         * A privilege was not revoked.
         */
        public static final Warning PrivilegeNotRevoked = new Warning(
                Constant.SqlStateSubclass.Warning.PrivilegeNotRevoked);
        /**
         * The query expression of the view is too long for the information schema.
         */
        public static final Warning QueryExpressionTooLongForInformationSchema =
                new Warning(
                Constant.SqlStateSubclass.Warning.QueryExpressionTooLongForInformationSchema);
        /**
         * The search condition is too long for the information schema.
         */
        public static final Warning SearchConditionTooLongForInformationSchema =
                new Warning(
                Constant.SqlStateSubclass.Warning.SearchConditionTooLongForInformationSchema);
        /**
         *
         */
        public static final Warning StatementTooLongForInformationSchema =
                new Warning(
                Constant.SqlStateSubclass.Warning.StatementTooLongForInformationSchema);
        /**
         * The value of a string was truncated when assigned to another string
         * data type with a shorter length.
         */
        public static final Warning StringDataRightTruncation = new Warning(
                Constant.SqlStateSubclass.Warning.StringDataRightTruncation);
    }

    /**
     *
     */
    @SuppressWarnings("PublicInnerClass")
    public static abstract class Exception extends SqlState {

        private static final long serialVersionUID = 1L;

        /**
         *
         * @param sqlStateClass
         * @param sqlStateSubclass
         */
        protected Exception(final String sqlStateClass,
                final String sqlStateSubclass) {
            super(sqlStateClass, sqlStateSubclass);

            if (Category != Constant.SqlStateCategory.Exception) {
                throw new IllegalArgumentException(
                        "sqlStateClass must denote an exception condition: "
                        + sqlStateClass);
            }
        }

        /**
         *
         */
        public static final class AmbiguousCursorName extends Exception {

            private static final long serialVersionUID = 1L;

            /**
             *
             * @param sqlStateSubclass
             */
            public AmbiguousCursorName(final String sqlStateSubclass) {
                super(Constant.SqlStateClass.AmbiguousCursorName,
                        sqlStateSubclass);
            }
            /**
             *
             */
            public static final AmbiguousCursorName NoSubclass =
                    new AmbiguousCursorName(
                    Constant.SqlStateSubclass.NoSubclass);
        }

        /**
         *
         */
        public static final class AttemptToAssignToNonUpdatableColumn
                extends Exception {

            private static final long serialVersionUID = 1L;

            /**
             *
             * @param sqlStateSubclass
             */
            public AttemptToAssignToNonUpdatableColumn(
                    final String sqlStateSubclass) {
                super(Constant.SqlStateClass.AttemptToAssignToNonUpdatableColumn,
                        sqlStateSubclass);
            }
            public final AttemptToAssignToNonUpdatableColumn NoSubClass =
                    new AttemptToAssignToNonUpdatableColumn(
                    Constant.SqlStateSubclass.NoSubclass);
        }

        /**
         *
         */
        public static final class AttemptToAssignToOrderingColumn extends Exception {

            private static final long serialVersionUID = 1L;

            /**
             *
             * @param sqlStateSubclass
             */
            public AttemptToAssignToOrderingColumn(
                    final String sqlStateSubclass) {
                super(Constant.SqlStateClass.AttemptToAssignToOrderingColumn,
                        sqlStateSubclass);
            }
        }

        public static final class CardinalityViolation extends Exception {

            private static final long serialVersionUID = 1L;

            /**
             *
             * @param sqlStateSubclass
             */
            public CardinalityViolation(final String sqlStateSubclass) {
                super(Constant.SqlStateClass.CardinalityViolation,
                        sqlStateSubclass);
            }
            /**
             *
             */
            public static final CardinalityViolation NoSubclass =
                    new CardinalityViolation(
                    Constant.SqlStateSubclass.NoSubclass);
        }

        /**
         *
         */
        public static final class ConnectionException extends Exception {

            private static final long serialVersionUID = 1L;

            /**
             *
             * @param sqlStateSubclass
             */
            public ConnectionException(final String sqlStateSubclass) {
                super(Constant.SqlStateClass.ConnectionException,
                        sqlStateSubclass);
            }
            /**
             *
             */
            public static final ConnectionException NoSubclass =
                    new ConnectionException(
                    Constant.SqlStateSubclass.NoSubclass);
        }

        public static final class InvalidCursorState extends Exception {

            private static final long serialVersionUID = 1L;

            /**
             *
             * @param sqlStateSubclass
             */
            public InvalidCursorState(final String sqlStateSubclass) {
                super(Constant.SqlStateClass.InvalidCursorState,
                        sqlStateSubclass);
            }
            /**
             *
             */
            public static final InvalidCursorState NoSubclass =
                    new InvalidCursorState(Constant.SqlStateSubclass.NoSubclass);
            public static final InvalidCursorState IdentifiedCursorIsNotOpen =
                    new InvalidCursorState(Constant.SqlStateSubclass.InvalidCursorState.IdentifiedCursorIsNotOpen);
            public static final InvalidCursorState IdentifiedCursorIsAlreadyOpen =
                    new InvalidCursorState(Constant.SqlStateSubclass.InvalidCursorState.IdentifiedCursorIsAlreadyOpen);
            public static final InvalidCursorState CannotFetch_NEXT_PRIOR_CURRENT_or_RELATIVE_CursorPositionIsUnknown =
                    new InvalidCursorState(Constant.SqlStateSubclass.InvalidCursorState.CannotFetch_NEXT_PRIOR_CURRENT_or_RELATIVE_CursorPositionIsUnknown);
            public static final InvalidCursorState IdentifiedCursorNotPositionedOnRowIn_UPDATE_DELETE_SET_or_GET_Statement =
                    new InvalidCursorState(Constant.SqlStateSubclass.InvalidCursorState.IdentifiedCursorNotPositionedOnRowIn_UPDATE_DELETE_SET_or_GET_Statement);
            public static final InvalidCursorState AllColumnsMustBeSetBeforInsert =
                    new InvalidCursorState(Constant.SqlStateSubclass.InvalidCursorState.AllColumnsMustBeSetBeforInsert);
            public static final InvalidCursorState RowHasBeenModifiedOutsideTheCursor =
                    new InvalidCursorState(Constant.SqlStateSubclass.InvalidCursorState.RowHasBeenModifiedOutsideTheCursor);
            public static final InvalidCursorState CursorDisabledByPreviousError =
                    new InvalidCursorState(Constant.SqlStateSubclass.InvalidCursorState.CursorDisabledByPreviousError);
        }
    }

    /**
     *
     */
    @SuppressWarnings("PublicInnerClass")
    public interface Constant {

        /**
         *
         */
        public static final int SQLSTATE_CONDITION_BUNDLE_ID =
                ResourceBundleHandler.getBundleHandle("sqlstate-condition",
                SqlState.class.getClassLoader());
        /**
         *
         */
        public static final int SQLSTATE_SUBCONDITION_BUNDLE_ID =
                ResourceBundleHandler.getBundleHandle("sqlstate-subcondition",
                SqlState.class.getClassLoader());

        /**
         *
         */
        public interface SqlStateCategory {

            char SuccessfulCompletion = 'S';
            char NoData = 'N';
            char Warning = 'W';
            char Exception = 'X';
        }

        /**
         *
         */
        public interface SqlStateClass {

            String AmbiguousCursorName = "3C";
            String AttemptToAssignToNonUpdatableColumn = "0U";
            String AttemptToAssignToOrderingColumn = "0V";
            String CardinalityViolation = "21";
            String ConnectionException = "08";
            String CursorSensitivityException = "36";
            String DataException = "22";
            String DependentPrivilegeDescriptorsStillExist = "2B";
            String DiagnosticsException = "0Z";
            String DynamicSQLError = "07";
            String ExternalRoutineException = "38";
            String ExternalRoutineInvocationException = "39";
            String FeatureNotSupported = "0A";
            String IntegrityConstraintViolation = "23";
            String InvalidAuthorizationSpecification = "28";
            String InvalidCatalogName = "3D";
            String InvalidCharacterSetName = "2C";
            String InvalidConditionNumber = "35";
            String InvalidConnectionName = "2E";
            String InvalidCursorName = "34";
            String InvalidCursorState = "24";
            String InvalidGrantor = "0L";
            String InvalidRoleSpecification = "0P";
            String InvalidSchemaName = "3F";
            String InvalidSchemaNameListSpecification = "0E";
            String InvalidCollationName = "2H";
            String InvalidSQLDescriptorName = "33";
            String InvalidSQLInvokedProcedureReference = "0M";
            String InvalidSQLStatementName = "26";
            String InvalidSQLStatementIdentifier = "30";
            String InvalidTargetTypeSpecification = "0D";
            String InvalidTransactionState = "25";
            String InvalidTransactionTermination = "2D";
            String InvalidTransformGroupNameSpecification = "0S";
            String NoData = "02";
            String ProhibitedStatementEncounteredDuringTriggerExecution = "0W";
            String RemoteDatabaseAccess = "HZ";
            String SavepointException = "3B";
            String SQLRoutineException = "2F";
            String SuccessfulCompletion = "00";
            String SyntaxErrorOrAccessRuleViolation = "42";
            String TargetTableDisagreesWithCursorSpecification = "0T";
            String TransactionRollback = "40";
            String TriggeredActionException = "09";
            String TriggeredDataChangeViolation = "27";
            String Warning = "01";
            String WithCheckOptionViolation = "44";

            public interface ODBC2 {

                String GeneralError = "S1";
            }

            public interface ODBC3 {

                String Odbc3GeneralError = "HY";
            }

            public interface PSM {

                String CaseNotFoundForCaseStatement = "20";
                String DataException = SqlStateClass.DataException;
                String DiagnosticsException = SqlStateClass.DiagnosticsException;
                String ResignalWhenHandlerNotActive = "0K";
                String UnhandledUserDefinedException = "45";
                String Warning = SqlStateClass.Warning;
            }

            public interface MED {

                String CLISpecificCondition = "HY";
                String DataException = SqlStateClass.DataException;
                String DatalinkException = "HW";
                String FDWSspecificCondition = "HV";
                String InvalidForeignServerSpecification = "0X";
                String PassThroughSpecificCondition = "0Y";
            }

            public interface OLB {

                String OLBSpecificError = "46";
            }

            public interface JRT {

                String JavaDDL = "46";
                String JavaExecution = "46";
                String Warning = SqlStateClass.Warning;
            }

            public interface XML {

                String DataException = SqlStateClass.DataException;
                String SQLXMLMappingError = "0N";
                String XQueryError = "10";
                String Warning = SqlStateClass.Warning;
            }
        }

        /**
         *
         */
        public interface SqlStateSubclass {

            String NoSubclass = "000";

            public interface ConnectionException {

                String ConnectionDoesNotExist = "003";
                String ConnectionFailure = "006";
                String ConnectionNameInUse = "002";
                String SQLClientUnableToEstablishSQLConnection = "001";
                String SQLServerRejectedEstablishmentOfSQLConnection = "004";
                String TransactionResolutionUnknown = "007";
            }

            public interface CursorSensitivityException {

                String RequestFailed = "002";
                String RequestRejected = "001";
            }

            public interface DataException {

                String ArrayDataRightTruncation = "02F";
                String ArrayElementError = "02E";
                String AttemptToReplaceAZeroLengthString = "01U";
                String CharacterNotInRepertoire = "021";
                String DatetimeFieldOverflow = "008";
                String DivisionByZero = "012";
                String ErrorInAssignment = "005";
                String EscapeCharacterConflict = "00B";
                String IndicatorOverflow = "022";
                String IntervalFieldOverflow = "015";
                String IntervalValueOutOfRange = "00P";
                String InvalidArgumentForNaturalLogarithm = "01E";
                String InvalidArgumentForPowerFunction = "01F";
                String InvalidArgumentForWidthBucketFunction = "01G";
                String InvalidCharacterValueForCast = "018";
                String InvalidDatetimeFormat = "007";
                String InvalidEscapeCharacter = "019";
                String InvalidEscapeOctet = "00D";
                String InvalidEscapeSequence = "025";
                String InvalidIndicatorParameterValue = "010";
                String InvalidIntervalFormat = "006";
                String InvalidParameterValue = "023";
                String InvalidPrecedingOrFollowingSizeInWindowFunction = "013";
                String InvalidRegularExpression = "01B";
                String InvalidRepeatErgumentInASampleClause = "02G";
                String InvalidSampleSize = "02H";
                String InvalidTimeZoneDisplacementValue = "009";
                String InvalidUseOfEscapeCharacter = "00C";
                String InvalidXQueryOptionFlag = "01T";
                String InvalidXQueryRegularExpression = "01S";
                String InvalidXQueryReplacementString = "01V";
                String MostSpecificTypeMismatch = "00G";
                String MultisetValueOverflow = "00Q";
                String NoncharacterInUCSString = "029";
                String NullValueSubstitutedForMutatorSubjectParameter = "02D";
                String NullRowNotPermittedInTable = "01C";
                String NullValueInArrayTarget = "00E";
                String NullValueNoIndicatorParameter = "002";
                String NullValueNotAllowed = "004";
                String NumericValueOutOfRange = "003";
                String SequenceGeneratorLimitExceeded = "00H";
                String StringDataLengthMismatch = "026";
                String StringDataRightTruncation = "001";
                String SubstringError = "011";
                String TrimError = "027";
            }

            public interface DiagnosticsException {

                String MaximumNumberOfStackedDiagnosticsAreasExceeded = "001";
            }

            public interface DynamicSQLError {

                String CursorSpecificationCannotBeExecuted = "003";
                String DataTypeTransformFunctionViolation = "00B";
                String InvalidDATATarget = "00D";
                String InvalidDATETIME_INTERVAL_CODE = "00F";
                String InvalidDescriptorCount = "008";
                String InvalidDescriptorIndex = "009";
                String InvalidLEVELValue = "00E";
                String PreparedStatementNotACursorSpecification = "005";
                String RestrictedDataTypeAttributeViolation = "006";
                String UndefinedDATAValue = "00C";
                String UsingClauseDoesNotMatchDynamicParameterSpecifications = "001";
                String UsingClauseDoesNotMatchTargetSpecifications = "002";
                String UsingClauseRequiredForDynamicParameters = "004";
                String UsingClauseRequiredForResultFields = "007";
            }

            public interface ExternalRoutineException {

                String ContainingSQLNotPermitted = "001";
                String ModifyingSQLDataNotPermitted = "002";
                String ProhibitedSQLStatementAttempted = "003";
                String ReadingSQLDataNotPermitted = "004";
            }

            public interface InvalidCursorState {

                String IdentifiedCursorIsNotOpen = "501";
                String IdentifiedCursorIsAlreadyOpen = "502";
                String IdentifiedCursorNotPositionedOnRowIn_UPDATE_DELETE_SET_or_GET_Statement = "504";
                String CannotFetch_NEXT_PRIOR_CURRENT_or_RELATIVE_CursorPositionIsUnknown = "513";
                String CursorDisabledByPreviousError = "514";
                String AllColumnsMustBeSetBeforInsert = "515";
                String RowHasBeenModifiedOutsideTheCursor = "521";
            }

            public interface NoData {

                String NoAdditionalResultSetsReturned = "001";
            }

            public interface Warning {

                String AdditionalResultSetsReturned = "00D";
                String ArrayDataRightTruncation = "02F";
                String AttemptToReturnTooManyResultSets = "00E";
                String CursorOperationConflict = "001";
                String DefaultValueTooLongForInformationSchema = "00B";
                String DisconnectError = "002";
                String ResultSetsReturned = "00C";
                String InsufficientItemDescriptorAreas = "005";
                String NullValueEliminatedInSetFunction = "003";
                String PrivilegeNotGranted = "007";
                String PrivilegeNotRevoked = "006";
                String QueryExpressionTooLongForInformationSchema = "00A";
                String SearchConditionTooLongForInformationSchema = "009";
                String StatementTooLongForInformationSchema = "00F";
                String StringDataRightTruncation = "004";
            }
        }
    }

    static final class Routine {

        /**
         *
         * @param sqlStateClass
         * @return
         */
        public static String conditionForSqlStateClass(final String sqlStateClass) {
            return ResourceBundleHandler.getString(Constant.SQLSTATE_CONDITION_BUNDLE_ID,
                    sqlStateClass);
        }

        /**
         *
         * @param sqlState
         * @return
         */
        public static String subConditionForSqlState(final String sqlState) {
            return ResourceBundleHandler.getString(Constant.SQLSTATE_SUBCONDITION_BUNDLE_ID,
                    sqlState);
        }

        /**
         *
         * @param value
         * @return
         */
        public static char categoryForSqlState(final String value) {
            SqlState.Routine.checkSqlState(value);

            if (value.startsWith(Constant.SqlStateClass.NoData)) {
                return Constant.SqlStateCategory.NoData;
            } else if (value.startsWith(Constant.SqlStateClass.SuccessfulCompletion)) {
                return Constant.SqlStateCategory.SuccessfulCompletion;
            } else if (value.startsWith(Constant.SqlStateClass.Warning)) {
                return Constant.SqlStateCategory.Warning;
            } else {
                return Constant.SqlStateCategory.Exception;
            }
        }

        /**
         *
         * @param sqlStateCategory
         */
        public static void checkSqlStateCategory(final char sqlStateCategory) {
            switch (sqlStateCategory) {
                case Constant.SqlStateCategory.Exception:
                case Constant.SqlStateCategory.NoData:
                case Constant.SqlStateCategory.SuccessfulCompletion:
                case Constant.SqlStateCategory.Warning: {
                    // all's good.
                    break;
                }
                default: {
                    throw new IllegalArgumentException("sqlStateCategory: "
                            + sqlStateCategory);
                }
            }
        }

        /**
         *
         * @param sqlState
         * @throws IllegalArgumentException if <tt>sqlstate</tt> is null or
         *  <tt>sqlstate</tt> length is not 5
         */
        public static void checkSqlState(final String sqlState) {
            if (sqlState == null) {
                throw new IllegalArgumentException("sqlState must not be null");
            } else if (sqlState.length() != 5) {
                throw new IllegalArgumentException("sqlState length must be 5");
            } else {
                for (int i = 0; i < 5; i++) {
                    if (!SqlState.Routine.isLegalSqlStateChar(sqlState.charAt(i))) {
                        throw new IllegalArgumentException(
                                "Illegal character encountered: "
                                + sqlState.charAt(i)
                                + " in sqlState: "
                                + sqlState);
                    }
                }
            }
        }

        public static boolean isLegalSqlStateChar(final char ch) {
            return ((ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9'));
        }

        public static boolean isReservedForStandardDefinedCondition(
                final char ch) {
            return (ch >= '0' && ch <= '4') || (ch >= 'A' && ch <= 'H');
        }

        public static boolean isReservedForImplementationDefinedCondition(
                final char ch) {
            return (ch >= '5' && ch <= '9') || (ch >= 'I' && ch <= 'Z');
        }

        /**
         *
         * @param sqlStateClass
         */
        public static void checkSqlStateClass(final String sqlStateClass) {
            if (sqlStateClass == null) {
                throw new IllegalArgumentException(
                        "SqlState class must not be null");
            } else if (sqlStateClass.length() != 2) {
                throw new IllegalArgumentException(
                        "SqlState class must be precisely 2 characters");
            } else {
                for (int i = 0; i < 2; i++) {
                    if (!SqlState.Routine.isLegalSqlStateChar(sqlStateClass.charAt(i))) {
                        throw new IllegalArgumentException(
                                "Illegal character encountered: "
                                + sqlStateClass.charAt(i)
                                + " in sqlStateClass: "
                                + sqlStateClass);
                    }
                }
            }
        }

        /**
         *
         * @param sqlStateSubclass
         */
        public static void checkSqlStateSubclass(final String sqlStateSubclass) {
            if (sqlStateSubclass == null) {
                throw new IllegalArgumentException(
                        "SqlState subclass must not be null");
            } else if (sqlStateSubclass.length() != 3) {
                throw new IllegalArgumentException(
                        "SqlState subclass must be precisely 3 characters");
            } else {
                for (int i = 0; i < 3; i++) {
                    if (!SqlState.Routine.isLegalSqlStateChar(sqlStateSubclass.charAt(i))) {
                        throw new IllegalArgumentException(
                                "Illegal character encountered: "
                                + sqlStateSubclass.charAt(i)
                                + " in sqlStateSubclass: "
                                + sqlStateSubclass);
                    }
                }
            }
        }

        /**
         *
         * @param value
         * @return
         */
        public static boolean isStandardDefinedConditionSqlState(
                final String value) {
            SqlState.Routine.checkSqlState(value);

            return isStandardDefinedConditionSqlState(value.substring(0, 2), value.substring(2));
        }

        /**
         *
         * @param value
         * @return
         */
        public static boolean isImplementationDefinedConditionSqlState(
                final String value) {
            SqlState.Routine.checkSqlState(value);

            return isImplementationDefinedConditionSqlState(value.substring(0, 2), value.substring(2));
        }

        /**
         *
         * @param sqlStateClass
         * @param sqlStateSubclass
         * @return
         */
        public static boolean isStandardDefinedConditionSqlState(
                final String sqlStateClass,
                final String sqlStateSubclass) {
            return SqlState.Routine.isStandardDefinedConditionSqlStateClass(sqlStateClass)
                    && SqlState.Routine.mustBeImplementationDefinedConditionSqlStateSubclass(sqlStateSubclass);
        }

        /**
         *
         * @param sqlStateClass
         * @param sqlStateSubclass
         * @return
         */
        public static boolean isImplementationDefinedConditionSqlState(
                final String sqlStateClass,
                final String sqlStateSubclass) {
            return SqlState.Routine.isImplementationDefinedConditionSqlStateClass(sqlStateClass)
                    || (SqlState.Routine.isStandardDefinedConditionSqlStateClass(sqlStateClass)
                    && SqlState.Routine.mustBeImplementationDefinedConditionSqlStateSubclass(sqlStateClass));
        }

        /**
         *
         * @param sqlStateClass
         * @return
         */
        public static boolean isStandardDefinedConditionSqlStateClass(
                final String sqlStateClass) {
            checkSqlStateClass(sqlStateClass);
            return SqlState.Routine.isReservedForStandardDefinedCondition(sqlStateClass.charAt(0));
        }

        /**
         *
         * @param sqlStateClass
         * @return
         */
        public static boolean canBeStandardDefinedConditionSqlStateSubclass(
                final String sqlStateSublass) {
            checkSqlStateSubclass(sqlStateSublass);
            return SqlState.Routine.isReservedForStandardDefinedCondition(
                    sqlStateSublass.charAt(0));
        }

        /**
         *
         * @param sqlStateClass
         * @return
         */
        public static boolean isImplementationDefinedConditionSqlStateClass(
                final String sqlStateClass) {
            checkSqlStateClass(sqlStateClass);
            return SqlState.Routine.isReservedForImplementationDefinedCondition(
                    sqlStateClass.charAt(0));
        }

        /**
         *
         * @param sqlStateClass
         * @return
         */
        public static boolean mustBeImplementationDefinedConditionSqlStateSubclass(
                final String sqlStateSubclass) {
            checkSqlStateSubclass(sqlStateSubclass);

            final char ch = sqlStateSubclass.charAt(0);

            return (ch >= '5' && ch <= '9') || (ch >= 'I' && ch <= 'Z');
        }

        /**
         *
         * @param sqlStateSubclass
         * @return
         */
        public static boolean isNoSubclass(final String sqlStateSubclass) {
            return Constant.SqlStateSubclass.NoSubclass.equals(sqlStateSubclass);
        }
    }
}
