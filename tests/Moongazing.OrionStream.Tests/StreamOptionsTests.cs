namespace Moongazing.OrionStream.Tests;

using System;

using Moongazing.OrionStream;

using Xunit;

public sealed class StreamOptionsTests
{
    [Fact]
    public void Defaults_are_256_capacity_and_15_second_heartbeat()
    {
        var options = new StreamOptions();

        Assert.Equal(256, options.SubscriberCapacity);
        Assert.Equal(TimeSpan.FromSeconds(15), options.HeartbeatInterval);
        Assert.Equal(256, options.ReplayBufferCapacity);
    }

    [Fact]
    public void A_zero_replay_buffer_capacity_is_valid()
    {
        var options = new StreamOptions { ReplayBufferCapacity = 0 };

        options.Validate();
    }

    [Fact]
    public void A_negative_replay_buffer_capacity_is_rejected()
    {
        var options = new StreamOptions { ReplayBufferCapacity = -1 };

        var ex = Assert.Throws<ArgumentOutOfRangeException>(options.Validate);
        Assert.Equal(nameof(StreamOptions.ReplayBufferCapacity), ex.ParamName);
    }

    [Fact]
    public void Default_options_validate_successfully()
    {
        var options = new StreamOptions();

        // Validate is internal; reachable via InternalsVisibleTo. No throw means valid.
        options.Validate();
    }

    [Fact]
    public void Capacity_of_one_is_valid()
    {
        var options = new StreamOptions { SubscriberCapacity = 1 };

        options.Validate();
    }

    [Fact]
    public void Zero_capacity_is_rejected()
    {
        var options = new StreamOptions { SubscriberCapacity = 0 };

        var ex = Assert.Throws<ArgumentOutOfRangeException>(options.Validate);
        Assert.Equal(nameof(StreamOptions.SubscriberCapacity), ex.ParamName);
    }

    [Fact]
    public void Negative_capacity_is_rejected()
    {
        var options = new StreamOptions { SubscriberCapacity = -5 };

        var ex = Assert.Throws<ArgumentOutOfRangeException>(options.Validate);
        Assert.Equal(nameof(StreamOptions.SubscriberCapacity), ex.ParamName);
    }

    [Fact]
    public void Zero_heartbeat_interval_is_rejected()
    {
        var options = new StreamOptions { HeartbeatInterval = TimeSpan.Zero };

        var ex = Assert.Throws<ArgumentOutOfRangeException>(options.Validate);
        Assert.Equal(nameof(StreamOptions.HeartbeatInterval), ex.ParamName);
    }

    [Fact]
    public void Negative_heartbeat_interval_is_rejected()
    {
        var options = new StreamOptions { HeartbeatInterval = TimeSpan.FromSeconds(-1) };

        var ex = Assert.Throws<ArgumentOutOfRangeException>(options.Validate);
        Assert.Equal(nameof(StreamOptions.HeartbeatInterval), ex.ParamName);
    }

    [Fact]
    public void A_one_tick_heartbeat_interval_is_valid()
    {
        var options = new StreamOptions { HeartbeatInterval = TimeSpan.FromTicks(1) };

        options.Validate();
    }

    [Fact]
    public void Default_full_buffer_policy_is_drop_oldest()
    {
        Assert.Equal(FullBufferPolicy.DropOldest, new StreamOptions().FullBufferPolicy);
    }

    [Fact]
    public void Wait_policy_without_a_cap_is_rejected()
    {
        var options = new StreamOptions { FullBufferPolicy = FullBufferPolicy.Wait };

        var ex = Assert.Throws<ArgumentException>(options.Validate);
        Assert.Equal(nameof(StreamOptions.MaxPublishWait), ex.ParamName);
    }

    [Fact]
    public void Wait_policy_with_a_positive_cap_is_valid()
    {
        var options = new StreamOptions
        {
            FullBufferPolicy = FullBufferPolicy.Wait,
            MaxPublishWait = TimeSpan.FromSeconds(1),
        };

        options.Validate();
    }

    [Fact]
    public void A_non_positive_max_publish_wait_is_rejected()
    {
        var options = new StreamOptions
        {
            FullBufferPolicy = FullBufferPolicy.Wait,
            MaxPublishWait = TimeSpan.Zero,
        };

        var ex = Assert.Throws<ArgumentOutOfRangeException>(options.Validate);
        Assert.Equal(nameof(StreamOptions.MaxPublishWait), ex.ParamName);
    }

    [Fact]
    public void A_slow_consumer_threshold_below_one_is_rejected()
    {
        var options = new StreamOptions
        {
            SlowConsumerPolicy = new SlowConsumerPolicy { MaxConsecutiveFullPublishes = 0 },
        };

        var ex = Assert.Throws<ArgumentOutOfRangeException>(options.Validate);
        Assert.Equal(nameof(SlowConsumerPolicy.MaxConsecutiveFullPublishes), ex.ParamName);
    }

    [Fact]
    public void A_per_topic_subscriber_capacity_override_below_one_is_rejected()
    {
        var options = new StreamOptions();
        options.ConfigureTopic("busy", o => o.SubscriberCapacity = 0);

        var ex = Assert.Throws<ArgumentOutOfRangeException>(options.Validate);
        // The invalid argument is the override's SubscriberCapacity, not the topic name.
        Assert.Equal(nameof(TopicCapacityOverride.SubscriberCapacity), ex.ParamName);
    }

    [Fact]
    public void A_per_topic_replay_capacity_override_below_zero_is_rejected()
    {
        var options = new StreamOptions();
        options.ConfigureTopic("busy", o => o.ReplayBufferCapacity = -1);

        var ex = Assert.Throws<ArgumentOutOfRangeException>(options.Validate);
        // The invalid argument is the override's ReplayBufferCapacity, not the topic name.
        Assert.Equal(nameof(TopicCapacityOverride.ReplayBufferCapacity), ex.ParamName);
    }

    [Fact]
    public void Configure_topic_amends_the_same_override_across_calls()
    {
        var options = new StreamOptions { SubscriberCapacity = 2, ReplayBufferCapacity = 2 };
        options.ConfigureTopic("busy", o => o.SubscriberCapacity = 9);
        options.ConfigureTopic("busy", o => o.ReplayBufferCapacity = 7);

        Assert.Equal(9, options.SubscriberCapacityFor("busy"));
        Assert.Equal(7, options.ReplayBufferCapacityFor("busy"));
        // An unconfigured topic keeps the global defaults.
        Assert.Equal(2, options.SubscriberCapacityFor("other"));
        Assert.Equal(2, options.ReplayBufferCapacityFor("other"));
    }
}
