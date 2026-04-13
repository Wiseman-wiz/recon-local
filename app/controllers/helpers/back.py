def attach_multiple_matches(self, df):
        """Attaching to df the matched multiple entries and creates breakdown."""
        df["mult"] = None
        mult_ent_transref_gl = list(
            self.multiple_entries_gl["transaction_reference"].unique()
        )
        mult_ent_transref_bs = list(
            self.multiple_entries_bs["transaction_reference"].unique()
        )
        for ref in mult_ent_transref_gl:
            ref_data = self.get_multiple_entries_breakdown(ref, "gl")
            df.loc[df.transaction_reference == ref, "mult"] = (
                str(ref_data) if ref_data else None
            )
        for ref2 in mult_ent_transref_bs:
            # for many to many we need to append to string formed list of dictionaries
            ref2_data = str(self.get_multiple_entries_breakdown(ref2, "bs"))
            df.loc[df.transaction_reference == ref2, "mult"] = (
                str(ref2_data) if ref2_data else None
            )
        return df