namespace Moongazing.OrionStream.Demo;

/// <summary>
/// Tiny console helper so every feature demo prints with a consistent, readable layout.
/// </summary>
internal static class DemoConsole
{
    public static void Header(string title)
    {
        Console.WriteLine();
        Console.WriteLine(new string('=', 72));
        Console.WriteLine($"  {title}");
        Console.WriteLine(new string('=', 72));
    }

    public static void Step(string text) => Console.WriteLine($"  > {text}");

    public static void Detail(string text) => Console.WriteLine($"      {text}");

    /// <summary>Print a wire-format payload with newlines made visible so framing is obvious.</summary>
    public static void Wire(string label, string wire)
    {
        Console.WriteLine($"  {label}:");
        var visible = wire.Replace("\n", "\\n\n        ", StringComparison.Ordinal);
        Console.WriteLine($"        {visible}");
    }
}
